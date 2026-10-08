// Checks which tags may become releases. A wrong tag must fail the release workflow before it
// creates anything, since a published release is hard to take back.
//
// No server and no model key. Usage: npx tsx checks/release-tag-check.mts

import { parseReleaseTag } from "../scripts/release-tag.mjs";

const failures: string[] = [];
const check = (what: string, ok: boolean, detail?: unknown) => {
  console.log(`${ok ? "PASS" : "FAIL"} ${what}${ok ? "" : ` (${JSON.stringify(detail)})`}`);
  if (!ok) failures.push(what);
};

const released = (tag: string, version = tag.slice(1)) => parseReleaseTag(tag, version);

for (const tag of ["v0.1.0", "v1.2.3", "v10.20.30"]) {
  const release = released(tag);
  check(`${tag} is a release`, typeof release === "object" && !release.prerelease, release);
}
for (const tag of ["v1.0.0-rc.1", "v1.0.0-alpha", "v1.0.0-0.3.7", "v1.0.0-x-y.1"]) {
  const release = released(tag);
  check(`${tag} is a pre-release`, typeof release === "object" && release.prerelease, release);
}
// SemVer forbids leading zeros, and build metadata would let two tags name one release.
for (const tag of [
  "1.2.3",
  "v1.2",
  "v1.2.3.4",
  "v01.2.3",
  "v1.02.3",
  "v1.2.3-",
  "v1.2.3-01",
  "v1.2.3+build.1",
  "v1.2.3-rc..1",
  "release-1.2.3",
]) {
  check(`${tag} is refused`, typeof released(tag) === "string", released(tag));
}
const mismatch = parseReleaseTag("v1.2.3", "1.2.2");
check(
  "a tag that doesn't match package.json is refused",
  typeof mismatch === "string" && /package.json says 1.2.2/.test(mismatch),
  mismatch,
);

const bad = failures.length;
console.log(bad === 0 ? "release-tag-check: OK" : `release-tag-check: ${bad} failed`);
process.exit(bad === 0 ? 0 : 1);
