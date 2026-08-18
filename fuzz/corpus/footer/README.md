Regression inputs for the `footer` decoder: bytes that once crashed it, kept so they cannot come back.

Seeds are **not** here. They are encoded at build time by `fuzz/seed_corpus.cpp`, because a
committed seed silently rots the next time the format moves — see that file.

Add an input here only with the fix that made it pass, and name it after the symptom.
