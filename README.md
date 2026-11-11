# Figures for the dialog guide

This branch holds what draws the guide's figures, kept off `main`. It shares
no history with `main`.

## `file/`: the "One File per Space" chapter

`guide/src/file.md` on the main line is generated from here:

- `chapter.md` is the chapter's text, with a `{{name}}` where each figure goes.
- `figs.py` draws each figure, one function per name, with the helpers in
  `svg.py`. The values in it (offsets, ids, hashes, bytes) are copied from
  `run.txt`.
- `run.txt` is the saved output of the run the chapter shows:
  `cargo run -p dialog-archive-file --example guide_bytes` on the dialog
  branch the chapter was written on. Hashes, keys and offsets differ from
  run to run, so a new run means updating the values in `figs.py` to match.
- `check.js` opens the built book in Chromium and reports figure text that
  is clipped by its figure or runs past its box.

To rebuild the chapter, from a dialog checkout:

```sh
python3 path/to/this/branch/file/build.py guide/src/file.md
mdbook build guide
node path/to/this/branch/file/check.js "$PWD/book/file.html" /tmp light 1200
```

Edit `chapter.md` and `figs.py` here, not `file.md` there: a rebuild writes
over hand edits.
