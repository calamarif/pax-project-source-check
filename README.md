# pax-project-source-check

This script will check all of the projects in your tenant match the underlying library schema. The only two variables necessary are the:
1) paxata_rest_token
2) paxata_url 

Obviously if you want to check ALL projects in the tenant, you will need access to them, so using superuser is a way to ensure that.

The three things being checked are:
1) Consistency between Project schema and the library schema
2) That all datatypes in the project are valid
3) That the display names of columns have not been changed (future enhancement will be to add a "rename" step in the event something doesn't match)
