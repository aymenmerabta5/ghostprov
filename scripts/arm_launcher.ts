/** Arm the canonical LATEXMKRC_SLOTS launcher into the doc. */
import "dotenv/config";
import { writeDocFile } from "../src/prism/ydoc";
import { LATEXMKRC_SLOTS } from "../src/relay/slots";

await writeDocFile(".latexmkrc", LATEXMKRC_SLOTS);
console.log("armed LATEXMKRC_SLOTS:\n" + LATEXMKRC_SLOTS);
process.exit(0);
