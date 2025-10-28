def combine_data_from_api(results):
    """Combine parsed gameweek results from multiple seasons into one dictionary."""
    combined = []
    for r in results:
        if r:
            combined.extend(r)
    return combined
