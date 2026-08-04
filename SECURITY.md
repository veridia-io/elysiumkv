# Security Policy

## Status

**ElysiumKV has had no external security review.** The README says the same thing, and it is repeated
here so it is not discovered late: treat this as software to evaluate, not as a hardened dependency.

## Supported versions

Only the latest release is supported. Versions come from git tags, and there are no maintenance
branches — a fix ships in the next tag rather than being backported.

## Reporting a Vulnerability

If you discover a security issue, please report it privately to avoid premature disclosure. You can
reach the security team at oss@veridia.io.

When reporting, please include:

- A description of the vulnerability and its impact
- Steps to reproduce or a proof of concept
- Any relevant logs, configuration, or a failing input file

## Disclosure Expectations

- We will acknowledge receipt of your report within 5 business days.
- We will investigate and work to provide a remediation plan or fix.
- Please allow a reasonable amount of time for us to address the issue before making any public
  disclosure.

## What is in scope

ElysiumKV is an embedded library: it opens no ports and runs no server, so the attack surface is
whatever the host process hands it. In scope:

- **Malformed or hostile stored data.** Every SST, manifest record and cached object is parsed, and
  the parsers are reached by anything that can write to the configured stores. A crafted file that
  causes a crash, an out-of-bounds read, or an unbounded allocation is a vulnerability — the length
  and CRC checks exist precisely to bound this, so a bypass of them is interesting.
- **Keys and values from untrusted sources**, including sizes at or beyond the documented limits.
- **Memory-safety faults** reachable through the public C++ API, the C ABI, or the Java binding.
  Pin and iterator lifetime handling is the most likely place for one.
- **The C ABI boundary**: an exception escaping it, or a handle misuse that corrupts state rather
  than returning an error.

## What is out of scope

- **Encryption at rest or in transit.** The engine provides neither. Storage-level encryption is the
  embedder's responsibility — an S3 bucket policy, an encrypted volume.
- **Access control.** Anything that can reach the configured blob store and manifest catalog can read
  and rewrite the database. That is a property of an embedded store, not a defect.
- **Two writers on one store.** Ownership is arbitrated by a single compare-and-set on the manifest
  pointer; a second writer is fenced at its next manifest write, not prevented from starting. Running
  two writers is a misconfiguration, and the data loss that can follow is documented, not a
  vulnerability.
- **`reclaim_orphans_at_open`.** It deletes unreferenced objects and is off by default precisely
  because open cannot detect a concurrent writer. Enabling it asserts exclusivity; losing data by
  enabling it while another process holds the store is the documented consequence.
- **Denial of service through configuration**, such as a memory budget too small for the configured
  instances. The engine sheds and stalls by design.

## What helps a report land

The test suite already runs under AddressSanitizer, UndefinedBehaviorSanitizer and
ThreadSanitizer, and there is a fault-injection harness that can corrupt or truncate stored objects
on demand. A reproducer expressed as a test against those tools is the fastest route to a fix.
