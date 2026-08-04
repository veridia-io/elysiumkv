/* ARCHITECTURE.md "Negative controls" — the negative control's subject: a library that exports one legitimate
 * symbol and one that has no business being exported.
 *
 * The export gate exists to catch a statically linked dependency leaking into a
 * shared object, where it can interpose on a host process's own copy. That gate
 * has silently degraded twice in this codebase — once when hidden visibility
 * swallowed the whole ABI, once when a build-system quoting slip truncated the
 * prefix list to its first entry — and both times a human noticed rather than
 * the build. This fixture makes the build notice.
 */

#if defined(_WIN32)
#  define FIXTURE_API __declspec(dllexport)
#else
#  define FIXTURE_API __attribute__((visibility("default")))
#endif

FIXTURE_API int elysiumkv_fixture_expected(void) { return 1; }

/* The stray. A gate that passes this is not checking anything. */
FIXTURE_API int definitely_not_a_elysiumkv_symbol(void) { return 2; }
