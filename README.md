# Movie Reposter

Drives `@UwinzawnyuntBot` through its own `/newpost` flow. The bot publishes to its channel itself; this worker never posts to the channel directly.

Flow: `/cancel` -> `/newpost` -> poster -> synopsis -> `a` -> all video files -> `a`.

State (cursor, done, failed) is stored in MongoDB so the worker can restart and resume. When every source post has been handled, it sends one Telegram notice to `NOTIFY_CHAT`.

Environment variables are set in Render; no sessions or keys are committed.
