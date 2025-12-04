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
        if gcnode == 'sample':
            loadline['participant.study_participant_id'] =f"{studyid}_{ first_result[gcnode]['participant.participant_id']}"
        elif gcnode == 'participant':
            loadline['study_participant_id'] =f"{studyid}_{first_result[gcnode]['participant_id']}"
        elif gcnode == 'sequencing_file':
            loadline['study.study_id'] = studyid
            participantid = sample2Participant(first_result[gcnode]['sample.sample_id'], conn)
            loadline['participant.study_participant_id'] =f"{studyid}_{participantid}"
        elif gcnode == 'diagnosis':
            loadline['participant.study_participant_id'] =f"{studyid}_{ first_result[gcnode]['participant.participant_id']}"
            loadline['study_diagnosis_id'] = f"{studyid}_{first_result[gcnode]['diagnosis_id']}"
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


