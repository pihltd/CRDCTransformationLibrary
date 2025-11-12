import bento_mdf

def getKeyProperty(node, mdf=None, mdffiles=None):
    '''Given a node and and mdf modle, returns a list of the key properties for that node.  In a sane world, there should only be one element in that list. Also note that if both an MDF object and MDF Files are provided, the files are ignored
    
    :param node: The node name to search for key properties
    :type: string
    :param mdf: An MDF model object (Optional)
    :type mdf: MDF model object
    :param mdffiles: A list of MDF compliant YAML files
    :type mdffiles: list of string
    :return: List of key properties
    :rtype: list of string
    '''

    keys = []
    proplist = None
    if mdf is not None:
        proplist = mdf.model.nodes[node].props
    elif mdffiles is not None:
        mdf = bento_mdf.MDF(*mdffiles)
        proplist = mdf.model.nodes[node].props
    if proplist is not None:
        for prop in proplist:
            if mdf.model.props[(node,prop)].get_attr_dict()['is_key'] == 'True':
                keys.append(prop)
    return keys


def getPropertyList(node, mdf=None, mdffiles=None):
    '''Given a node and and mdf modle, returns a list of the properties for that node.  Note that if both an MDF object and MDF Files are provided, the files are ignored
    
    :param node: The node name to search for key properties
    :type: string
    :param mdf: An MDF model object (Optional)
    :type mdf: MDF model object
    :param mdffiles: A list of MDF compliant YAML files (Optional)
    :type mdffiles: list of string
    :return: List of properties
    :rtype: list of string
    '''

    proplist = []
    if mdf is not None:
        proplist = list(mdf.model.nodes[node].props)
    elif mdffiles is not None:
        mdf = bento_mdf.MDF(*mdffiles)
        proplist =list( mdf.model.nodes[node].props)
    return proplist