add 
| to_entries 
|  map(select(.key |endswith(":V") )) 
# | map(select(.value[0]> 40))  
| map(
  {
    keyword: .key, keyword_count: .value[0]
  } + (.value[3:][] | 
        {
          pattern: .[0],
          pattern_count:.[1] , 
          instances: .[2] | map(  {instance:  .[0] | split("[")[1] | split("]")[0]  , tag: .[2]  } )
        }
      )
)
