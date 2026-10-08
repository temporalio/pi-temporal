// Decides whether a pushed tag may become a release. The release workflow runs it, and
// `checks/release-tag-check.mts` covers its rules.
//
// A tag is `v` and a SemVer 2.0 version, such as `v1.2.3` or `v1.2.3-rc.1`. Build metadata
// (`+...`) is refused, since SemVer ignores it when it orders versions, and two tags that differ
// only there would name one release. The version must equal `package.json`'s, so what installs
// from the tag reports the version it was tagged with. A version with a pre-release part is
// released as a GitHub pre-release.
//
// Usage: npx tsx scripts/release-tag.mts <tag>. Prints `version=...` and `prerelease=...` lines
// for `$GITHUB_OUTPUT`, or exits 1 with the reason.

import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const NUMBER = "(?:0|[1-9]\\d*)";
const IDENTIFIER = `(?:${NUMBER}|\\d*[A-Za-z-][0-9A-Za-z-]*)`;
const CORE = `${NUMBER}\\.${NUMBER}\\.${NUMBER}`;
const PRERELEASE = `${IDENTIFIER}(?:\\.${IDENTIFIER})*`;
const TAG = new RegExp(`^v(${CORE})(?:-(${PRERELEASE}))?$`);

export interface ReleaseTag {
  readonly version: string;
  readonly prerelease: boolean;
}

/** The release a tag names, or the reason it names none. */
export function parseReleaseTag(tag: string, packageVersion: string): ReleaseTag | string {
  const match = TAG.exec(tag);
  if (!match) {
    return (
      `${tag} is not a release tag. ` +
      "Use v<major>.<minor>.<patch>, with an optional -<pre-release>."
    );
  }
  const version = tag.slice(1);
  if (version !== packageVersion) {
    return `${tag} names ${version}, but package.json says ${packageVersion}. Bump it first.`;
  }
  return { version, prerelease: match[2] !== undefined };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const tag = process.argv[2] ?? "";
  const pkg = JSON.parse(
    await readFile(fileURLToPath(new URL("../package.json", import.meta.url)), "utf8"),
  ) as { version: string };
  const release = parseReleaseTag(tag, pkg.version);
  if (typeof release === "string") {
    console.error(release);
    process.exit(1);
  }
  console.log(`version=${release.version}`);
  console.log(`prerelease=${release.prerelease}`);
}
