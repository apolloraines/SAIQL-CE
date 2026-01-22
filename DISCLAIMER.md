Disclaimer: Database conversion is not magic



Databases are inherently different. “Any-DB to Any-DB” does not mean 100% lossless, 100% automatic conversion for every schema, feature, and vendor-specific behavior. SAIQL’s goal is to make migrations safer and more predictable by proving what will and won’t convert cleanly before anything is written.



SAIQL is Proof-first, not promise-first: SAIQL generates explicit artifacts (diffs, mappings, and warnings) that show exactly what changes in the destination. If a conversion is lossy, SAIQL will say so up front. “Lossy” means representation or constraint changes (e.g., precision/scale differences, length constraints removed, collations/encodings differing, function/trigger/sequence semantics changing), not silent deletion. The source database is treated as read-only during analysis; your original data is not modified. You decide whether the tradeoffs are acceptable before applying any write plan.



Success depends on scope: Schema-only conversions, common types, and standard SQL features typically migrate cleanly; vendor-specific features may require adapters, transforms, or manual decisions. If SAIQL cannot confidently prove correctness for a feature, it will mark it unsupported or require an explicit override.



