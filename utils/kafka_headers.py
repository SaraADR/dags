def extract_trace_id(message):
    """
    Kafka headers processor (Node.js style)
    Returns: 
        tuple: (trace_id, status_message) 
              - trace_id: str|None
              - status_message: str (for logging)
    """
    # Early return if no headers
    if not (headers := getattr(message, 'headers', lambda: None)()):
        return None, "No headers available"
    
    # Header processing
    for key, value in headers:
        key_str = key.decode('utf-8') if isinstance(key, bytes) else key
        value_str = value.decode('utf-8') if isinstance(value, bytes) else value
        
        if key_str == 'traceId':
            return value_str, f"TraceId found: {value_str}"
    
    return None, "No traceId in headers"