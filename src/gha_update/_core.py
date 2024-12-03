from __future__ import annotations

import os
import tomllib
import typing as t
from asyncio import Task
from asyncio import TaskGroup
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from httpx import AsyncClient
from httpx import Response

class GHRateLimitException(Exception):
    """Raised when github hits an API rate limit"""

Config = t.TypedDict(
    "Config",
    {
        "tag-only": list[str],
    },
)
default_config: Config = {
    "tag-only": [],
}


def load_config_path(path: os.PathLike[str] | str) -> Config:
    data = tomllib.loads(Path(path).read_text("utf-8"))
    config = data.get("tool", {}).get("gha-update", {})
    return {**default_config, **config}


async def update_workflows(config: Config | None = None) -> None:
    if config is None:
        config = default_config

    workflows = read_workflows()
    actions: set[str] = set()

    for path_actions in workflows.values():
        actions.update(path_actions)

    versions = await get_versions(actions)
    write_workflows(config, workflows, versions)


def iter_workflows() -> Iterator[Path]:
    for path in (Path.cwd() / ".github" / "workflows").iterdir():
        if not (path.name.endswith(".yaml") or path.name.endswith(".yml")):
            continue

        yield path


def read_workflows() -> dict[Path, set[str]]:
    out: dict[Path, set[str]] = {}

    for path in iter_workflows():
        out[path] = set()

        for line in path.read_text("utf-8").splitlines():
            if (name := find_name_in_line(line)) is None:
                continue

            out[path].add(name)

    return out


def find_name_in_line(line: str) -> str | None:
    uses = line.partition(" uses:")[2].strip()

    # ignore other lines, and local and docker actions
    if not uses or uses.startswith("./") or uses.startswith("docker://"):
        return None

    parts = uses.partition("@")[0].split("/")

    # repo must be owner/name
    if len(parts) < 2:
        return None

    # omit subdirectory
    return "/".join(parts[:2])

async def make_requests(name_repo_pairs: list[dict[str, str]]) -> dict[str, Task[Response]]:
    tasks: dict[str, Task[Response]] = {}
    headers: dict[str, str] = {}
    if github_token := os.environ.get("GITHUB_TOKEN"):
        headers["Authorization"] = github_token
    async with AsyncClient(base_url="https://api.github.com", headers=headers) as c, TaskGroup() as tg:
        for name_repo_pair in name_repo_pairs:
            tg.create_task(c.get(f"/repos/{name_repo_pair["repo"]}/tags"))
    return tasks

def resolve_responses(tasks: dict[str, Task[Response]]) -> tuple[dict[str,
    tuple[str, str]], list[dict[str, str]]]:
    out: dict[str, tuple[str, str]] = {}
    redirected_name_repo_pairs: list[dict[str, str]] = []

    for name, task in tasks.items():
        if task.result().status_code == 403:
            raise(GHRateLimitException(task.result().json()))
        #handle redirects
        if task.result().status_code in [301, 302, 307]:
            print(task.result().headers.get("location"))
            redirected_name = task.result().headers.get("location")
            redirected_name_repo_pairs.append({"name": name, "repo": redirected_name})
        else:
            out[name] = highest_version(task.result().json())

    return out, redirected_name_repo_pairs

async def get_versions(names: Iterable[str]) -> dict[str, tuple[str, str]]:
    name_repo_pairs: list[dict[str, str]] = [{"name": n, "repo": n} for n in names]

    tasks = make_requests(name_repo_pairs)
    out, redirected_pairs = resolve_responses(tasks)
    while redirected_pairs:
        tasks = make_requests(redirected_pairs)
        redirected_out, redirected_pairs = resolve_responses(tasks)
        out.update(redirected_out)
    return out


def highest_version(tags: Iterable[Mapping[str, Any]]) -> tuple[str, str]:
    items: dict[str, str] = {t["name"]: t["commit"]["sha"] for t in tags}
    versions: dict[tuple[int, ...], str] = {}

    for name in items:
        try:
            parts = tuple(int(p) for p in name.removeprefix("v").split("."))
        except ValueError:
            continue

        versions[parts] = name

    version = versions[max(versions)]
    return version, items[version]


def write_workflows(
    config: Config, paths: Iterable[Path], versions: Mapping[str, tuple[str, str]]
) -> None:
    for path in paths:
        out: list[str] = []

        for line in path.read_text("utf-8").splitlines():
            if (name := find_name_in_line(line)) is not None and name in versions:
                left, _, right = line.partition("@")
                tag, commit = versions[name]

                if name in config["tag-only"]:
                    line = f"{left}@{tag}"
                else:
                    line = f"{left}@{commit} # {tag}"

            out.append(line)

        out.append("")
        path.write_text("\n".join(out), "utf-8")
