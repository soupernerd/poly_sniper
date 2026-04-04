# MOO Notes

## Engineering Principle (Persistent)

- Always peel back and fix the core cause instead of patching symptoms.
- Prefer simpler control flow over layered conditional patches.
- If behavior looks wrong, diagnose the data flow and state transitions first, then change logic.
- Keep HFT hot-path code minimal and explicit: less branching, fewer hidden interactions, clearer logs.
