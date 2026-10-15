import { extname } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * A module beside `base`, with `base`'s own extension. The source runs as `.ts` under tsx and
 * Pi's loader, and the published package runs the compiled `.js`, so a fixed extension would
 * name a file that one of them doesn't have.
 */
export const siblingModule = (base: string, path: string): string =>
  fileURLToPath(new URL(`${path}${extname(new URL(base).pathname)}`, base));
