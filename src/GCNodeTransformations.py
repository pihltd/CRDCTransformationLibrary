import pandas as pd
import cypherQueryBuilders as cqb


def loadProps(proplist, transform_df, loadline, result, querynode):
    for prop in proplist:
        if prop in transform_df['lift_from_prop'].unique().tolist():
            prop_df = transform_df.query('lift_from_prop == @prop')
            for index, row in prop_df.iterrows():
                loadline[row['lift_to_prop']] = result[querynode][prop]
    return loadline


def getStudyID(node, field, conn):
    query = cqb.cypherGetBasicNodeQuery(node)
    results = conn.query(query=query, db='neo4j')
    studyId = results[0][node][field]
    return studyId


def sample2Participant(sampleid, conn):
    
    query = f"MATCH (s:SAMPLE) WHERE s.sample_id = '{sampleid}' RETURN s"
    results = conn.query(query=query)
    return results[0]['s']['participant.participant_id']

def gcGeneralLineNode(gcnode, to_df, transform_df, conn, dbnodelist, studyid = None):
    # Basic approach to line-by-line loading
    # Useful for GC node

    from_node_list = transform_df['lift_from_node'].unique().tolist()
    from_node_list.remove(gcnode)
    first_query = cqb.cypherGetNodeQuery(gcnode)
    first_results = conn.query(query=first_query)
    for first_result in first_results:
        loadline = {}
        elids = []
        from_properties = list(first_result[gcnode].keys())
        loadline = loadProps(from_properties, transform_df, loadline, first_result, gcnode)
        elids.append({gcnode: first_result['elid']})
        #
        # Node specific loadings happen here
        #
        if gcnode in ['sample', 'diagnosis' ]:
            loadline['participant.study_participant_id'] =f"{studyid}_{ first_result[gcnode]['participant.participant_id']}"
        elif gcnode == 'participant':
            loadline['study_participant_id'] =f"{studyid}_{first_result[gcnode]['participant_id']}"
        elif gcnode == 'sequencing_file':
            loadline['study.study_id'] = studyid
            participantid = sample2Participant(first_result[gcnode]['sample.sample_id'], conn)
            loadline['participant.study_participant_id'] =f"{studyid}_{participantid}"
        #
        # Secondary node loop.  I'm still not sure this is the right way to do it.
        #
        for secondary_node in from_node_list:
            if secondary_node.upper() in dbnodelist:
                secondary_query = cqb.cypherGetNodeQuery(secondary_node)
                secondary_results = conn.query(query=secondary_query)
                for secondary_result in secondary_results:
                    from_properties = list(secondary_result[secondary_node].keys())
                    loadline = loadProps(from_properties, transform_df, loadline, secondary_result, secondary_node)
                    elids.append({secondary_node: secondary_result['elid']})
        loadline['parent_elementId'] = elids
        to_df.loc[len(to_df)] = loadline
    return to_df


'''def gcStudyNode2(to_df, transform_df, conn):
    CCDI to GC Migration of the GC Study node

    :param to_df: The dataframe where the transformation will be stored
    :type to_df: Pandas dataframe
    :param transform_df: The dataframe containg the CCDI to GC mapping for GC Study node
    :type transform_df: Pandas dataframe
    :param conn:  Connection to the Neo4j database
    :type conn: Python-Neo4j connection object
    :return: An updated copy of the to_df dataframe
    :rtype: Pandas dataframe

    gcnode = 'study'

    from_node_list = transform_df['lift_from_node'].unique().tolist()
    from_node_list.remove(gcnode)
    to_query = cqb.cypherGetNodeQuery(gcnode)
    main_results = conn.query(query=to_query)
    for mainresult in main_results:
        loadline = {}
        elids = []
        from_properties = list(mainresult[gcnode].keys())
        loadline = loadProps(from_properties, transform_df, loadline, mainresult, gcnode)
        elids.append({gcnode: mainresult['elid']})
    # At this point, the study node has been pulled over, now loop through additional nodes
    for secondary_node in from_node_list:
         secquery = cqb.cypherGetNodeQuery(secondary_node)
         secondary_results = conn.query(query=secquery)
         for secresult in secondary_results:
            from_properties = list(secresult[secondary_node].keys())
            loadline = loadProps(from_properties, transform_df, loadline, secresult, secondary_node)
            elids.append({secondary_node: secresult['elid']})
    loadline['parent_elementId'] = elids
    to_df.loc[len(to_df)] = loadline
    return to_df'''


def gcStudyNode(to_df, transform_df, conn, dbnodelist):
    to_df = gcGeneralLineNode('study', to_df, transform_df, conn, dbnodelist)
    return to_df

def gcSampleNode(to_df, transform_df, conn, dbnodelist):
    studyid = getStudyID('study', 'study_id', conn)
    to_df = gcGeneralLineNode('sample', to_df, transform_df, conn, dbnodelist, studyid)
    return to_df

def gcParticipantNode(to_df, transform_df, conn, dbnodelist):
    studyid = getStudyID('study', 'study_id', conn)
    to_df = gcGeneralLineNode('participant', to_df, transform_df, conn, dbnodelist, studyid)
    return to_df

def gcDiagnosisNode(to_df, transform_df, conn, dbnodelist):
    studyid = getStudyID('study', 'study_id', conn)
    to_df = gcGeneralLineNode('diagnosis', to_df, transform_df, conn, dbnodelist, studyid)
    return to_df

def gcFileNode(to_df, transform_df, conn, dbnodelist):
    studyid = getStudyID('study', 'study_id', conn)
    to_df = gcGeneralLineNode('sequencing_file', to_df, transform_df, conn, dbnodelist, studyid)
    return to_df

def gcGenomicInfoNode(to_df, transform_df, conn, dbnodelist):
    studyid = getStudyID('study', 'study_id', conn)
    to_df = gcGeneralLineNode('sequencing_file', to_df, transform_df, conn, dbnodelist, studyid)
    return to_df

#def gcImageNode(to_df, transform_df, conn, dbnodelist):
#    to_df = geGeneralLineNode('')


'''def gcSampleNode2(to_df, transform_df, conn):


    gcnode = 'sample'
    studyid = getStudyID('study', 'study_id', conn)
    from_node_list = transform_df['lift_from_node'].unique().tolist()
    from_node_list.remove(gcnode)
    to_query = cqb.cypherGetNodeQuery(gcnode)
    main_results = conn.query(query=to_query)
    for mainresult in main_results:
        loadline = {}
        elids = []
        from_properties = list(mainresult[gcnode].keys())
        loadline = loadProps(from_properties, transform_df, loadline, mainresult, gcnode)
        # Add the study_participant_id
        loadline['participant.study_participant_id'] =f"{studyid}_{ mainresult[gcnode]['participant.participant_id']}"
        elids.append({gcnode: mainresult['elid']})
        # At this point, the study node has been pulled over, now loop through additional nodes
        for secondary_node in from_node_list:
            secquery = cqb.cypherGetNodeQuery(secondary_node)
            secondary_results = conn.query(query=secquery)
            for secresult in secondary_results:
                from_properties = list(secresult[secondary_node].keys())
                loadline = loadProps(from_properties, transform_df, loadline, secresult, secondary_node)
                elids.append({secondary_node: secresult['elid']})
        loadline['parent_elementId'] = elids
        to_df.loc[len(to_df)] = loadline
    return to_df'''


'''def gcParticipantNode2(to_df, transform_df, conn):

    gcnode = 'participant'
    studyid = getStudyID('study', 'study_id', conn)
    from_node_list = transform_df['lift_from_node'].unique().tolist()
    from_node_list.remove(gcnode)
    to_query = cqb.cypherGetNodeQuery(gcnode)
    main_results = conn.query(query=to_query)
    for mainresult in main_results:
        loadline = {}
        elids = []
        from_properties = list(mainresult[gcnode].keys())
        loadline = loadProps(from_properties, transform_df, loadline, mainresult, gcnode)
        elids.append({gcnode: mainresult['elid']})
        loadline['study_participant_id'] =f"{studyid}_{ mainresult[gcnode]['participant_id']}"
        loadline['parent_elementId'] = elids
        to_df.loc[len(to_df)] = loadline
    return to_df  '''



'''def gcDiagnosisNode2(to_df, transform_df, conn):


    gcnode = 'diagnosis'
    studyid = getStudyID('study', 'study_id', conn)
    from_node_list = transform_df['lift_from_node'].unique().tolist()
    from_node_list.remove(gcnode)
    to_query = cqb.cypherGetNodeQuery(gcnode)
    main_results = conn.query(query=to_query)
    for mainresult in main_results:
        loadline = {}
        elids = []
        from_properties = list(mainresult[gcnode].keys())
        loadline = loadProps(from_properties, transform_df, loadline, mainresult, gcnode)
        # Add the study_participant_id
        loadline['participant.study_participant_id'] =f"{studyid}_{ mainresult[gcnode]['participant.participant_id']}"
        elids.append({gcnode: mainresult['elid']})
        # At this point, the study node has been pulled over, now loop through additional nodes
        for secondary_node in from_node_list:
            secquery = cqb.cypherGetNodeQuery(secondary_node)
            secondary_results = conn.query(query=secquery)
            for secresult in secondary_results:
                from_properties = list(secresult[secondary_node].keys())
                loadline = loadProps(from_properties, transform_df, loadline, secresult, secondary_node)
                elids.append({secondary_node: secresult['elid']})
        loadline['parent_elementId'] = elids
        to_df.loc[len(to_df)] = loadline
    return to_df'''



'''def gcFileNode2(to_df, transform_df, conn, dbnodelist):


    gcnode = 'sequencing_file'
    studyid = getStudyID('study', 'study_id', conn)
    from_node_list = transform_df['lift_from_node'].unique().tolist()
    from_node_list.remove(gcnode)
    to_query = cqb.cypherGetNodeQuery(gcnode)
    main_results = conn.query(query=to_query)
    for mainresult in main_results:
        loadline = {}
        elids = []
        from_properties = list(mainresult[gcnode].keys())
        loadline = loadProps(from_properties, transform_df, loadline, mainresult, gcnode)
        loadline['study.study_id'] = studyid
        #print(f"Sending sample id: {mainresult[gcnode]['sample.sample_id']}")
        participantid = sample2Participant(mainresult[gcnode]['sample.sample_id'], conn)
        # Add the study_participant_id
        loadline['participant.study_participant_id'] =f"{studyid}_{participantid}"
        elids.append({gcnode: mainresult['elid']})
        # At this point, the study node has been pulled over, now loop through additional nodes
        for secondary_node in from_node_list:
            if secondary_node.upper() in dbnodelist:
                secquery = cqb.cypherGetNodeQuery(secondary_node)
                secondary_results = conn.query(query=secquery)
                for secresult in secondary_results:
                    from_properties = list(secresult[secondary_node].keys())
                    loadline = loadProps(from_properties, transform_df, loadline, secresult, secondary_node)
                    elids.append({secondary_node: secresult['elid']})
        loadline['parent_elementId'] = elids
        to_df.loc[len(to_df)] = loadline
    return to_df'''

'''def gcGenomicInfoNode2(to_df, transform_df, conn, dbnodelist):


    # Unlike other nodes, this is a different label name
    gcnode = 'sequencing_file'
    #studyid = getStudyID('study', 'study_id', conn)
    from_node_list = transform_df['lift_from_node'].unique().tolist()
    from_node_list.remove(gcnode)
    to_query = cqb.cypherGetNodeQuery(gcnode)
    main_results = conn.query(query=to_query)
    for mainresult in main_results:
        loadline = {}
        elids = []
        from_properties = list(mainresult[gcnode].keys())
        loadline = loadProps(from_properties, transform_df, loadline, mainresult, gcnode)
        # Add the study_participant_id
        #loadline['participant.study_participant_id'] =f"{studyid}_{ mainresult[gcnode]['participant.participant_id']}"
        elids.append({gcnode: mainresult['elid']})
        # At this point, the study node has been pulled over, now loop through additional nodes
        for secondary_node in from_node_list:
            if secondary_node.upper() in dbnodelist:
                secquery = cqb.cypherGetNodeQuery(secondary_node)
                secondary_results = conn.query(query=secquery)
                for secresult in secondary_results:
                    from_properties = list(secresult[secondary_node].keys())
                    loadline = loadProps(from_properties, transform_df, loadline, secresult, secondary_node)
                    elids.append({secondary_node: secresult['elid']})
        loadline['parent_elementId'] = elids
        to_df.loc[len(to_df)] = loadline
    return to_df'''


