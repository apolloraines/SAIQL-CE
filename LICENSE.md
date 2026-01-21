# Open Lore License v1.1 – "Community Edition (Non-Cloud)"

**SAIQL Community Edition (CE) Software License**

---

## Preamble

The **Open Lore License (OLL)** is a hybrid licensing model designed to foster innovation while ensuring fair compensation. This license grants broad freedom to builders of all sizes, while requiring commercial licensing for cloud deployments and hosted service offerings.

**Philosophy**: Ideas should lift others, not cage them. Build boldly, fly fairly.

---

## PART 1: SOFTWARE LICENSE (SAIQL Engine)

This Part 1 covers the **SAIQL database engine software** (code, binaries, documentation) in this repository.

**Community Edition note**: This license text is tailored for the CE distribution and includes a Non-Cloud restriction in Section 3.3.

### 1. Definitions

| Term | Definition |
|------|------------|
| **SAIQL** | The Semantic Artificial Intelligence Query Language database engine and related software contained in this repository. |
| **Licensee** | Any person or organization using SAIQL software. |
| **Public Implementation** | Any deployment of SAIQL that is accessible to end users via UI, API, web service, or distributed application. Internal-only use is not considered public. |
| **Hosted Service** | Offering SAIQL as a database-as-a-service (DBaaS), cloud database, or managed database service to third parties, whether free or paid. |
| **Public Cloud Provider** | A third-party multi-tenant cloud infrastructure provider that offers compute, storage, networking, or managed platform services on shared infrastructure, including (non-exhaustive) AWS, Azure, GCP, Oracle Cloud, Alibaba Cloud, IBM Cloud, and similar providers. |
| **Public Cloud Deployment** | Running SAIQL on infrastructure or managed services provided by a Public Cloud Provider (IaaS, PaaS, serverless, managed Kubernetes, managed databases, or equivalent), whether for internal use or external use. |
| **Private / On-Prem Deployment** | Running SAIQL on hardware or single-tenant infrastructure controlled by the Licensee, including on-premises servers, dedicated machines, airgapped environments, or dedicated hardware in colocation where the Licensee controls the instance and access. |

---

### 2. Grant of License (CE)

✅ **FREE USAGE** is granted to any person or organization to **use, modify, distribute, and create derivative works** of SAIQL Community Edition (CE), **subject to** the restrictions in Sections 3 (Hosted Service) and 3.3 (Non-Cloud Restriction), and the attribution requirements in Section 4.

**Commercial license is required** only for:
- Any **Hosted Service** offering (Section 3), regardless of revenue
- Any **Public Cloud Deployment** (Section 3.3), regardless of revenue
- Any additional rights not granted herein (e.g., certain trademark/branding usage, enterprise support/SLAs, or access to non-CE components)

**Contact**: apollo@saiql.ai for commercial licensing terms.

---

### 3. Hosted Service Clause

🌐 **Special Rule for Database-as-a-Service**:

Offering SAIQL as a hosted/managed database service to third parties requires a commercial license.

#### 3.1 Definition

**"Hosted/Managed Service"** means providing access to SAIQL's core functionality to third parties over a network where the third party is not an employee or contractor using it solely for the Licensee's internal business purposes.

#### 3.2 Examples

**Requires commercial license**:
- ✗ "SAIQL Cloud" - hosted database service for customers
- ✗ "Database-as-a-Service powered by SAIQL"
- ✗ Managed SAIQL instances offered to external clients
- ✗ API service where third parties query SAIQL directly

**Allowed under CE license**:
- ✓ Using SAIQL internally for your own application
- ✓ Building a SaaS application that uses SAIQL as its backend
- ✓ Distributing SAIQL as part of your open-source project
- ✓ Running SAIQL for your company's internal teams/tools

#### 3.3 Community Edition Non-Cloud Restriction

**Important (CE):** Public cloud deployments are **not permitted** under this Community Edition license.

- Running SAIQL CE on a **Public Cloud Provider** (IaaS/PaaS/serverless/managed Kubernetes/managed services) requires a **commercial license**.
- This restriction applies even for **internal use**.

**Allowed under CE**:
- Private / on-prem deployments (including dedicated single-tenant hardware and controlled colocation)
- Local development machines and private lab environments
- CI/build runners used **only** for building/testing (no production data, no third-party access to SAIQL functionality)

**Not allowed under CE**:
- Deploying or hosting SAIQL CE on AWS/Azure/GCP (or similar) for internal or external use
- Any deployment that exposes SAIQL CE's core query/engine functionality to third parties over a network (covered by Section 3.1 as Hosted Service)

#### 3.4 Contact for Hosted Services or Cloud Deployment

If you intend to offer SAIQL as a Hosted/Managed Service or deploy on public cloud infrastructure, contact apollo@saiql.ai to obtain written permission and applicable commercial terms.

**Rationale**: Prevents larger cloud providers from commoditizing SAIQL without contributing to its development.

---

### 4. Attribution Requirement

📢 **Public implementations must include attribution** in at least one of:

**Required locations** (choose minimum one):
- Application footer or about page
- API response headers (`X-Powered-By: SAIQL`)
- Documentation/README
- User-facing interface

**Attribution text** (choose one):
- "Powered by SAIQL"
- "Built with SAIQL"
- "Database: SAIQL (saiql.ai)"

**Exemption**: Internal-only deployments (not accessible to end users) do not require public attribution.

---

### 5. Anti-Circumvention

🚫 **You may not circumvent** the Hosted Service and Non-Cloud restrictions through restructuring, proxy hosting, "white-label" offerings, or similar arrangements.

Examples of circumvention include (non-exhaustive):
- Using affiliates or shell entities to offer SAIQL CE as a Hosted Service
- Using a third party to run SAIQL CE on a Public Cloud Provider "on your behalf" while you claim it is "private"
- Repackaging SAIQL CE as a managed service under a different name

**Intent**: Preserve the CE boundary and prevent commoditization via managed cloud/service offerings.

---

### 6. Enforcement and Termination

#### 6.1 Breach and Cure

License may be terminated if Licensee:
1. Violates the Non-Cloud or Hosted Service restrictions
2. Fails to provide required attribution
3. Attempts to circumvent license restrictions
4. Uses SAIQL for prohibited purposes (see ethical use)

**Cure Period**: 30 days written notice to remedy breach before termination.

#### 6.2 Survival

Obligations for attribution survive license termination.

---

### 7. Ethical Use

🛡️ **Encouraged principles** (not legally binding, but informing revocation decisions):

- **Transparent Development**: Open development practices
- **Ethical AI**: No weaponized AI, surveillance systems, or exploitative applications
- **Good Faith**: Honest licensing compliance

**Note**: Violations of ethical principles may be considered in commercial licensing decisions.

---

### 8. Enterprise Engagement

🦅 **Entities exceeding $1 billion USD** in annual gross revenue are encouraged to contact us for:

- Enterprise support and SLA options
- Additional terms for large-scale deployments
- Access to Full Edition features and components

This is not a license restriction—CE remains free for on-prem use regardless of company size. This section exists to facilitate conversations about enterprise needs.

**Contact**: apollo@saiql.ai for enterprise inquiries.

---

### 9. Modifications and Updates

Licensees may choose to adopt newer versions of this license or remain under v1.1.

**Current Version**: OLL v1.1-CE (Effective 2025)

---

## PART 2: PATENT AND TECHNOLOGY REFERENCE

This Part 2 references the broader **LoreTokens ecosystem** and underlying technology concepts.

### 10. LoreTokens Technology

SAIQL CE may incorporate concepts from the **LoreTokens** ecosystem (via LoreToken-Lite where applicable) for cognitive compression and memory management in artificial intelligence.

**Reference**: USPTO Provisional Patent Application
*System and Method for Hierarchical, Persistent, and Contextual AI Memory and Lore Management*

**Scope**: This license grants rights to use the SAIQL CE software. Separate licensing may apply for other LoreTokens ecosystem components (Lorechain, Full Edition features, etc.).

---

## License Summary Table

| Use Case | CE Status | Action Required |
|----------|-----------|-----------------|
| **Private / On-Prem Deployment** | ✅ **ALLOWED** | Include attribution where required |
| **Local development / CI build runners** | ✅ **ALLOWED** | No production hosting; follow Section 3.3 |
| **Hosted Service (any)** | 💼 **COMMERCIAL** | Obtain commercial license |
| **Public Cloud Deployment (any)** | 💼 **COMMERCIAL** | Obtain commercial license |

---

## Contact & Licensing

**Apollo Raines** - Creator & Rights Holder
**Email**: apollo@saiql.ai
**Web**: https://saiql.ai
**License**: https://openlorelicense.com/

---

## FAQ

**Q: Can I use SAIQL CE in my startup?**
A: Yes! CE has no revenue tiers. Use is allowed as long as you comply with the Hosted Service and Non-Cloud restrictions and attribution requirements.

**Q: Can I offer "SAIQL as a Service"?**
A: Only with a commercial license.

**Q: Can I run SAIQL CE on AWS/Azure/GCP?**
A: Not under this CE license. Public cloud deployment requires a commercial license (see Section 3.3).

**Q: What about open source projects?**
A: Fully allowed. Just include attribution.

**Q: Can I fork SAIQL CE?**
A: Yes, but your fork remains under OLL-CE with the same restrictions.

**Q: What counts as "internal use"?**
A: Using SAIQL for your own business operations, even if those operations serve customers. Example: A SaaS company using SAIQL CE as their backend database on their own servers is internal use (allowed under CE).

**Q: What counts as "Hosted Service"?**
A: Offering SAIQL's database functionality directly to third parties as a service. Example: "SAIQL Cloud" where customers can spin up SAIQL instances (requires commercial license).

**Q: Is there a revenue limit for CE?**
A: No. CE is free for on-prem/private deployments regardless of your company's revenue. The only restrictions are on cloud deployment and hosted services.

---

**Open Lore License v1.1-CE - Effective 2025**

*Build boldly. Fly fairly.*
