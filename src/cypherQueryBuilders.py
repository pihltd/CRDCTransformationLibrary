import os

def cypherLoadCSVQuery(nodelabel, filename, separator='tab', proplist = [], keyprop=None, nodeprefix=None):
    '''Creates a cypher query that will load data from a csv file

    :param nodelabel: The label for the nodes that will be created for the data in the csv file
    :type nodelabel: string
    :param filename: The full path and filename for the csv file
    :type filename: string
    :param separator: The type of separator used in the csv file.  Must be either 'tab' or 'csv'.
    :type separator: string Default 'tab'
    :param proplist: A list of properties to associate with the node
    :type proplist: list of string.  Default is empty list
    :param keyprop: The key property for the node.
    :type keyprop: string
    :return: A completed cypher query string for loading the csv file
    :rtype: string
    '''

    returnstring = None
    if keyprop is not None:
        startstring = f"LOAD CSV WITH HEADERS FROM 'file:///{os.path.basename(filename)}' AS row"
        if separator == 'tab':
            startstring = startstring+" FIELDTERMINATOR '\t'"
        if nodeprefix is not None:
            mergestring = f" MERGE ({nodelabel.lower()}:{nodeprefix}_{nodelabel.upper()} {{{keyprop}:row.{keyprop}}})"
        else:
            mergestring = f" MERGE ({nodelabel.lower()}:{nodelabel.upper()} {{{keyprop}:row.{keyprop}}})"
        returnstring = startstring+mergestring
        if keyprop in proplist:
            proplist.remove(keyprop)
        oncreatestring = " ON CREATE SET "
        proparray = []
        for prop in proplist:
            if "." in prop:
                prop = f"`{prop}`"
            proparray.append(f"{nodelabel.lower()}.{prop} = row.{prop}")
        returnstring = returnstring+oncreatestring+",".join(proparray)
    return returnstring




def cypherRelationshipQuery(srcnodelabel, dstnodelabel, edgelabel, keyproperty, modellabel=None):
    '''Creates a cypher query that creates an edge/relationsihp between a source node and a destination node

    :param srcnodelabel: The label for the existing source node
    :type srcnodelabel: string
    :param dstnodelabel: The label for the existing destination node
    :type dstnodelable: string
    :param edgelabel: The label to apply to newly created relationships/edges
    :type edgelable: string
    :param keyproperty: The property to match to create the edge
    :return: A completed cypher query creating an edge
    :rtype: string    
    '''

    if modellabel is not None:
        edgequery = f"MATCH ({srcnodelabel.lower()}:{srcnodelabel.upper()}), ({modellabel.lower()}_{dstnodelabel.lower()}:{modellabel.upper()}{dstnodelabel.upper()})"
    else:
        edgequery = f"MATCH ({srcnodelabel.lower()}:{srcnodelabel.upper()}), ({dstnodelabel.lower()}:{dstnodelabel.upper()})"
    #dstnodelabel is the problem
    if modellabel is not None:
        wherestring = f" WHERE {srcnodelabel.lower()}.`{dstnodelabel.lower()}.{keyproperty.lower()}` = {modellabel.lower()}_{dstnodelabel.lower()}.{keyproperty.lower()}"
    else:
        wherestring = f" WHERE {srcnodelabel.lower()}.`{dstnodelabel.lower()}.{keyproperty.lower()}` = {dstnodelabel.lower()}.{keyproperty.lower()}"
    edgequery = edgequery+wherestring
    if modellabel is not None:
        createstring = f" CREATE ({srcnodelabel.lower()})-[:{edgelabel}]->({modellabel.lower()}_{dstnodelabel.lower()})"
    else:
        createstring = f" CREATE ({srcnodelabel.lower()})-[:{edgelabel}]->({dstnodelabel.lower()})"
    edgequery = edgequery+createstring
    return edgequery



def cypherOfTransformRelationshipsQuery(srcnodelabel, dstnodelabel, edgelabel, srckeyprop):
    '''Creates a cypher query that adds a relationships between two nodes based on the node elementID.  This requires the source node to have a record of the destination node elementId.
    
    :param srcnodelabel: The label for the existing source node
    :type srcnodelabel: string
    :param dstnodelabel: The label for the existing destination node
    :type dstnodelable: string
    :param edgelabel: The label to apply to newly created relationships/edges
    :type edgelabel: string
    :param srckeyprop: The property from the source node contating the elementID that will be mapped to the destination nodes
    :type srckeyprop: string
    :return: A completed cypher query linking the source node to the destination node via matching elementId
    :rtype: string
    '''

    edgequery = f"MATCH ({srcnodelabel.lower()}:{srcnodelabel.upper()}), ({dstnodelabel.lower()}:{dstnodelabel.upper()})"
    wherestring = f" WHERE elementid({srcnodelabel.lower()}) = {dstnodelabel.lower()}.{srckeyprop}"
    edgequery = edgequery+wherestring
    createstring = f" CREATE ({srcnodelabel.lower()})-[:{edgelabel}]->({dstnodelabel.lower()})"
    edgequery = edgequery+createstring
    return edgequery


def cypherElementIDRelationshipQuery(parentlabel, childlabel, edgelabel, parentelementid, childemelentid):

    edgequery = f"MATCH ({parentlabel.lower()}:{parentlabel.upper()}), ({childlabel.lower()}:{childlabel.upper()})"
    wherestring = f" WHERE elementid({parentlabel.lower()}) = '{parentelementid}' AND elementid({childlabel.lower()}) = '{childemelentid}'"
    createstring = f" CREATE ({parentlabel.lower()})-[:{edgelabel}]->({childlabel.lower()})"
    edgequery = edgequery+wherestring+createstring
    return edgequery


def cypherUniqueLabels(dbconn, db='neo4j'):
    '''Returns a list of the labels/nodes found in the database

    :param dbconn: A database connection object
    :type dbconn: Neo4jConnection object
    :param db: The database to query.  Default is neo4j
    :type db: string
    :return: A list of the lables found in the database
    :rtype: list of string'''

    query = 'MATCH (n) RETURN distinct labels(n)'
    res = dbconn.query(query=query, db=db)
    final = []
    for each in res:
        final.append(each.data()['labels(n)'][0])
    return final

def cypherGetNodeQuery(node):
    '''Returns a query to get all instances of the node plus the elementIds in elid

    :param node: The name of the node to query
    :type node: string
    :return: The query needed to get all instances of the node
    :rtype: string'''

    return f"MATCH ({node.lower()}:{node.upper()}) WITH *, elementID({node.lower()}) AS elid RETURN {node.lower()},elid"

def cypherGetBasicNodeQuery(node):
    '''Similar to cypherGetNodeQuery, this one queryies just on the label, elementID is not included

    :param node: The name of the node to query
    :type node: string
    :return: The query needed to get all instances of the node
    :rtype: string'''

    return f"MATCH ({node.lower()}:{node.upper()}) RETURN {node.lower()}"

def cypherElementIDQuery(elementid):
    '''Returns a query that uses the node and elementId to find a specific node

    :param node: The name of the node to query
    :type node: string
    :param elementid: The database elementId to query
    :type elementid: string
    :return: The query needed to get all instances of the node with the provided elementId
    :rtype: string
    '''
    return f"MATCH (s) WHERE elementId(s) = '{elementid}' RETURN s" 

def cypherRecordCount(node):
    '''Returns a query for the number of  records for the provided node

    :param node: The name of the node to query
    :type node: string
    :return: The query needed to get all instances of the node
    :rtype: string'''

    return f"MATCH ({node.lower()}:{node.upper()}) RETURN COUNT(*) as count"


def cypherSingleWhereQuery(node, field, value):
    '''Returns a where with a single WHERE clause on the provided field and value
    
    :param node: The name of the node containing the field
    :type node: string
    :param field: The name of the field in the node to be queried
    :type field: string
    :param value: The value of the field to query for
    :type value: string
    :return: The query that will get the provided node for the field and value
    :rtype: string'''

    return f"MATCH ({node.lower()}:{node.upper()} WHERE {node.lower()}['{field.lower()}'] = '{value}') RETURN {node.lower()}"
    


