import { readFile, realpath, stat } from "node:fs/promises";
import { resolve, sep } from "node:path";
// Only installed, operator-owned game data is accessible to the world worker.
export async function dataReader(directory) {
  const root = await realpath(directory);
  return async (resource) => {
    if (
      typeof resource !== "string" ||
      !resource.startsWith("/") ||
      resource.startsWith("//") ||
      resource.includes("\\") ||
      resource.includes("\0")
    )
      throw Error("invalid game data path");
    const candidate = resolve(root, "." + resource);
    if (!candidate.startsWith(root + sep))
      throw Error("game data escaped root");
    let path;
    try {
      path = await realpath(candidate);
    } catch (error) {
      if (error.code === "ENOENT")
        return new Response("not found", { status: 404 });
      throw error;
    }
    if (!path.startsWith(root + sep)) throw Error("game data escaped root");
    const info = await stat(path);
    if (!info.isFile() || info.size > 256 * 1024 * 1024)
      throw Error("invalid game data file");
    return new Response(await readFile(path));
  };
}
