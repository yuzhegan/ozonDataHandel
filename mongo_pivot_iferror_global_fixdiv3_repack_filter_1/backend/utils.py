def flatten_value(v):
    if v is None: return None
    if isinstance(v,(list,tuple,set)): return ', '.join(map(str,v))
    if isinstance(v,dict): return str(v)
    return v

def flatten_doc(doc):
    return {k: flatten_value(v) for k,v in doc.items()}
