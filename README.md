# pax-project-source-check

This script will check all of the projects in your tenant match the underlying library schema. The only variables necessary are in *env_config.json* file (this can contain multiple configs although only one will be used during a single run):
1) *TENANT1* - Is the name in the example __env_config.json__ file
2) *PAXATA_CORE_SERVER* - Is the name of the environment you're connecting to
2) *PAXATA_REST_TOKEN* - Is where you paste the Paxata Rest API token (the users token you use will need to have access to all projects and library items) 

The three things being checked are:
1) Consistency between Project schema (for anchor and lookup tables) and the library schema
2) That the display names of columns have not been changed (future enhancement will be to add a "rename" step in the event something doesn't match)
3) Any projects with missing data sources (either the anchor table or lookup tables.)
