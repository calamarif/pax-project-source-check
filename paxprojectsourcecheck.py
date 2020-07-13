__author__ = 'Callum'
#paxprojectsourcecheck.py - Pax Project Source Check - checks the source of the schema - 
# Make sure you run with a user with access to ALL of the projects
#Python 3.7.3
#Version: 0.1
#Date: Jul 13th 2020

import requests, json, copy
from requests.auth import HTTPBasicAuth

def check_for_invalid_field_types(auth_token, paxata_url, project_schema_json):
    result = False
    for column in project_schema_json:
        if column['columnType']  not in ['Number','String','DateTime','Boolean']:
            print("INCORRECT DATATYPE IN PROJECT= " + column['columnType'])
            outF.write("INCORRECT DATATYPE IN PROJECT= " + column['columnType'])
            result = True
    return result



# (5) Get all the Projects that have been described with a specific "description_tag"
def get_all_project_information(auth_token, paxata_url):
    #Package_Tagged_Projects = []
    #package_counter = 0
    #max_num_of_projects = 0
    list_of_projects = []
    url_request = (paxata_url + "/projects")
    my_response = requests.get(url_request,auth=auth_token , verify=True)
    if(my_response.ok):
        list_of_projects = json.loads(my_response.content)
        #for item in jDataProjectIds:
        #    if description_tag == jDataProjectIds[package_counter].get('description'):
        #        ProjectNames.append(jDataProjectIds[package_counter].get('name'))
        #        Package_Tagged_Projects.append(jDataProjectIds[package_counter].get('projectId'))
        #        max_num_of_projects += 1
        #    package_counter += 1
    return list_of_projects

def get_name_and_schema_of_datasource(auth_token,paxata_url,libraryId,library_version):
    url_request = (paxata_url + "/library/data/"+str(libraryId)+"/"+str(library_version))
    my_response = requests.get(url_request, auth=auth_token, verify=True)
    jdata_datasources = []
    if(my_response.ok):
        jdata_datasources = json.loads(my_response.content)
    return jdata_datasources

def get_project_script(auth_token,paxata_url,projectId):
    url_request = (paxata_url + "/scripts?projectId=" + projectId + "&version=" + "-1")
    myResponse = requests.get(url_request, auth=auth_token)
    if (myResponse.ok):
        # Loads (Load String) takes a Json file and converts into python data structure (dict or list, depending on JSON)
        json_of_project = json.loads(myResponse.content)
    else:
        json_of_project = 0
        myResponse.raise_for_status()
    return(json_of_project)

# Update an existing Project's script file
def update_project_with_new_script(auth_token,paxata_url,updated_json_script,projectId):
    url_request = (paxata_url + "/scripts?update=script&force=true&projectId=" + str(projectId))
    s = {'script': json.dumps(updated_json_script)}
    myResponse = requests.put(url_request, data=s, auth=auth_token)
    result = False
    if (myResponse.ok):
        # json_of_existing_project = json.loads(myResponse.content)
        result = True
    else:
        #if there is a problem in updating the project, it would indicate a problem with the script, so lets output it
        print(myResponse.content)
        result = False
    return result

def run_a_project(auth_token,paxata_url,projectId):
    post_request = (paxata_url + "/project/publish?projectId=" + projectId)
    postResponse = requests.post(post_request, auth=auth_token)
    if (postResponse.ok):
        print("Project Run - " + projectId)
    else:
        print("Something went wrong with POST call " + str(postResponse))
    # I need to investigate the below, sometimes postResponse.content is a dict, sometimes a list, hence the two below trys
    try:
        AnswersetId = json.loads(postResponse.content)[0].get('dataFileId')
    except(AttributeError):
        AnswersetId = json.loads(postResponse.content).get('dataFileId', 0)
    return AnswersetId

# (3) POST Library data from Paxata and load it into a JSON structure
def get_paxata_library_data(auth_token,paxata_url,library_dataset_id):
    post_request = (paxata_url + "/datasource/exports/local/" + library_dataset_id + "?format=json")
    post_response = requests.post(post_request,auth=auth_token)
    JsonData = ""
    if (post_response.ok):
        JsonData = json.loads(post_response.content)
    return JsonData

def update_project_with_new_dataset(auth_token,paxata_url,updated_json_script,projectId):
    url_request = (paxata_url + "/rest/scripts?update=datasets&force=true&projectId=" + str(projectId))
    s = {'script': json.dumps(updated_json_script)}
    myResponse = requests.put(url_request, data=s, auth=auth_token)
    result = False
    print(myResponse)
    if (myResponse.ok):
        # json_of_existing_project = json.loads(myResponse.content)
        result = True
    else:
        #if there is a problem in updating the project, it would indicate a problem with the script, so lets output it
        print(myResponse.content)
        result = False
    return result



# Not used in this script
def insert_initial_data_into_empty_project(auth_token,paxata_url,json_of_existing_project,libraryId):
    #update the script... take the existing script and manipulate it.
    updated_json_script = copy.deepcopy(json_of_existing_project)
    updated_json_script['steps'][0]['importStep']['libraryId'] = str(libraryId)
    updated_json_script['steps'][0]['importStep']['libraryVersion'] = 1
    updated_json_script['steps'][0]['importStep']['libraryIdWithVersion'] = str(libraryId) + "_" + str(1)
    #function to get the metadata
    library_name,library_version,library_schema_dict = get_name_and_schema_of_datasource(auth_token,paxata_url,libraryId)
    i=0
    for schema_item in json_of_datasource_schema:
        temp_name = schema_item.get('name')
        temp_type = schema_item.get('type')
        updated_json_script['steps'][0]['importStep']['columns'].insert(i,{'hidden': False})
        updated_json_script['steps'][0]['importStep']['columns'][i]['columnDisplayName'] = temp_name
        updated_json_script['steps'][0]['importStep']['columns'][i]['columnType'] = temp_type
        updated_json_script['steps'][0]['importStep']['columns'][i]['columnName'] = temp_name
        #go to the next element
        i+=1

    return(updated_json_script)

def update_project_script_with_new_libraryid(temp_json_of_project, libraryId, library_version):
    #update the datasource id and version values (assumes the schema is the same)
    for step in temp_json_of_project['steps']:
        if (step['type']) == "AnchorTable":
            step['importStep']['libraryId'] = libraryId
            step['importStep']['libraryIdWithVersion'] = str(libraryId) + "_" + str(library_version)
            step['importStep']['libraryVersion'] = library_version
    return(temp_json_of_project)




def check_project():
    # *****************THESE ARE YO VARIABLES - YOU NEED TO EDIT THESE *******
    #dataprep.paxata.com
    paxata_rest_token = "4efac0a8b3494ad3b5139708321e477a"
    paxata_url = "https://datarobot.paxata.com/rest"
    # end of variables to set
   
    #check if someone has left off the rest part of the 
    # URL checking (making sure the /rest is on the URL)
    if paxata_url[-1:] == "/":
        paxata_url = paxata_url[0:-1]
    if paxata_url[-5:] != "/rest":
        paxata_url = paxata_url+"/rest"
    
    auth_token = HTTPBasicAuth("", paxata_rest_token)
    # open a (new) file to write
    outF = open("paxata_metadata_report.txt", "w")
    #1 get the projects in the tenant
    list_of_projects = get_all_project_information(auth_token, paxata_url)
    #list to hold the problem projectIds
    problem_projectIds = []
    print ("Number of Projects = " + str(len(list_of_projects)))
    outF.write("Paxata - Metadata report for " + paxata_url)
    outF.write("Number of Projects = " + str(len(list_of_projects)) + "\n")
    for project_json in list_of_projects:
        projectId = project_json.get('projectId')
        project_name = project_json.get('name')
        project_creator = project_json.get('userName')
        project_script = get_project_script(auth_token,paxata_url,projectId)
        #take a copy of the project so we can export out the entire script
        full_json_script = copy.deepcopy(project_script)
        # delete everything other than the first step in the project
        del project_script['steps'][1:]
        # main loop to go through each project
        for step in project_script['steps']:
            if (step['type']) == "AnchorTable":
                libraryId = step['importStep']['libraryId']
                library_version = step['importStep']['libraryVersion']
                project_schema = step['importStep']['columns']
                library_schema = ""
                # this is a function that will check that there are only valid values in field type (in the project script)
                valid_datatypes_in_project = check_for_invalid_field_types(auth_token, paxata_url, project_schema)
                # Get the project metadata
                jdata_datasources = get_name_and_schema_of_datasource(auth_token,paxata_url,libraryId,library_version)                
                # checking this first because there is the possibility that the schema is empty and everything else is populated
                if jdata_datasources:
                    # extract the specific library data for 
                    library_name = jdata_datasources.get('name')
                    library_schema = jdata_datasources.get('schema')
                    library_createTime = jdata_datasources.get('createTime')
                    library_source = jdata_datasources.get('source').get('type')
                    library_size = jdata_datasources.get('size')
                    library_rowcount = jdata_datasources.get('rowCount')
                    library_state = jdata_datasources.get('state')
                
                # Check for a deleted or no existing library
                if not library_schema:
                    print ("Schema is MISSING for Project: "+ project_name + ". WARNING: Dataset no longer exists or has not finished importing into the Library")
                    outF.write("Schema is MISSING for Project: "+ project_name + ". WARNING: Dataset no longer exists or has not finished importing into the Library" + "\n")
                else:
                    # remove two unnecessary metadata items from the library metadata
                    for field in library_schema:
                        try:
                            field.pop('columnTags')
                            field.pop('maxSize')
                            # rename library metadata to have the same keys as the project dict
                            field['columnType'] = field.pop('type')
                            field['columnName'] = field.pop('name')
                            field['columnDisplayName'] = field.pop('orignalColumnName')
                            # Some library "Hidden" data has None, some has False. Standardising it here to False.
                            if field['hidden'] == None:
                                field['hidden'] = False
                        except:
                            print ("Something went wrong manipulating the library schema " + library_source)

                    # Check if the schema's are different
                    if project_schema == library_schema:
                        print("Schemas are the same for Project: " + str(project_name))
                        outF.write("Schemas are the same for Project: " + str(project_name) + "\n")
                    else:
                        # add the projectId to the list of things that might have a problem
                        problem_projectIds.append(projectId)
                        print("Schemas are different for Project: " + project_name + " and " + library_name + " (Version:" + str(library_version) + " Created:"+library_createTime)
                        print(" from source:" + library_source + " . Rows = " + str(library_rowcount) + "(" + str(library_size) + "bytes). State: " + library_state + "\n")
                        
                        outF.write("Schemas are different for Project: " + project_name + " and " + library_name + " (Version:" + str(library_version) + " Created:"+library_createTime)
                        outF.write(" from source:" + library_source + " . Rows = " + str(library_rowcount) + "[" + str(library_size) + "bytes]. State: " + library_state + "\n")
                        outF.write("PROJECT SCHEMA = " + str(full_json_script) + "\n")
                        outF.write("LIBRARY SCHEMA = " + str(library_schema) + "\n")
                        
                        i =0
                        while len(project_schema) > i:
                            set1 = set(project_schema[i].items())
                            set2 = set(library_schema[i].items())
                            differences = set1 ^ set2
                            # check for an empty set and don't print it.
                            if differences:
                                outF.write("Difference between Project Script and Library Item = " + str(differences) + "\n")
                            i+=1
                                
                        # Also want to check for different display name and column name
                        for column in project_schema:
                            if not column['columnDisplayName'] == column['columnName']:
                                print ("WARNING, manually changed Project script metadata. Column: " + column['columnDisplayName'] + " is not equal to: " + column['columnName'] + " | in Project Name= " + project_name + " ProjectId= "+ projectId)
                                outF.write("WARNING, manually changed Project script metadata. Column: " + column['columnDisplayName'] + " is not equal to: " + column['columnName'] + " | in Project Name= " + project_name + " ProjectId= "+ projectId + "\n")
    outF.write("\n\n*** Summary. Projects analysed = "+ str(len(list_of_projects))+ "\n")
    outF.write("Potential problem projectIds are: " + str(problem_projectIds))
    outF.close()

if __name__ == "__main__":
    check_project()
