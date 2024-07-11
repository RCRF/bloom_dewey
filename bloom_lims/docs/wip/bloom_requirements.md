# Global

* Not aiming to be perfect
* Vigourously resisting conceptual shortcuts (ie: metadata in UUIDS, conflating objects: samples and contianers)
* Embracing non-standard data structures and allowsing them to define models/objects in a way the definition is shared
* Top level abstracted objects are more rigid, instantiations and subtypes are very flexible
* Users can define all instantiation and subtype templates
* Use some postgres tricks to get some non-ORM aligned functionality (global query)
* Audit trails of all changes tracked.
* soft delete approach used
* Embrace use of UUID and EUIDs, EUIDs are simply a core requirement of a viable physical system which requires labeling things with a UUID encoded barcode and human readable metadata.
* Resist special casing as much as possible
* Allow some encoding of object/schema/relationship info in the structure of the database schema itself...
* Offer an admin view of the database schema as well as direct editing of objects in the database.
* Keep all business logic in the ORM, period.  Avoid scripts or UIS which circumvent the ORM or implement anything that should be in the ORM.
* Build so that v2+ might be ported to other datastores: mysql seems an easy lift, salesforce would be the coup. A graph DB might be nice?
* I don't always know exactly what I'm doing, or if I do, and a noob as far as the domain (graph theory)... likely there are better ways to accomplish things. Be open to feedback.
* Scale is a must have.
* Performance is a must have.
* Security is a must have.
* Ease of use is a must have.
* Development might require a steeper learning curve, but should be as supported with examples and docs.
* Testing is a must have.
* Core workflows demonstrating the end to end system are a must have.
* Installable with pip. Docker or AWS AMI would be nice.
* Includes dashboards for monitoring and management.
* Includes reports for monitoring and management.
* Includes a CLI for monitoring and management (may be ORM at first)
* Can claim to be CLIA/CAP/HIPAA compliant
* Includes pre-composed validation templates for CLIA/CAP/HIPAA compliance
* Functional components to at least have examples of: Accessioning, test requisition capture, creating workflows (which define assays and the subworkflows needed to accomplish them), reagent tracking, assest/equipment tracking.
* DO NOT try to represent things upstream of the TReq, ie: patient, provider, etc.  This is a lab system, not a patient management system.
* Stub out data objects for analysis, have a template for inline QC data capture however.
* Be free and open source.
* Include development notes (chatgpt4 conversations, etc) in the repo.
* Publish ala Snakemake rolling paper, reference: https://f1000research.com/articles/10-33/v1