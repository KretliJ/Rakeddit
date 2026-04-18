### Lowest level auxiliary. 
    
    # Most basic element, extracts a JSON reddit endpoint response
    # url: string with request URL
    get_json(url)
    

### Second level auxiliary. 
    
    # Saves post json in ./json_dumps/subreddit_name or .ini defined default
    # data: data in json

    save_post(data)
    

### Harvests from chosen subreddit
    
    # subreddit_name: string with subreddit name (e.g. "anime")
    # category: string with category to search into (e.g. "top")
    # limit: maximum of posts to rake down the chain

    harvest_subreddit(subreddit_name, category, limit)
    
### Flattens a comment tree using Depth-First Search.

    # folder_path: Base path for subreddit folder (e.g. './json_dumps/')
    # limit: "none" to read all (default), or string with subreddit name (e.g. "anime")

    extract_from_post(folder_path, limit="none"):
    
    