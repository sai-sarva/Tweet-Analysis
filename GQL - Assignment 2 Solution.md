//Creating the Tweet Nodes
CALL apoc.periodic.iterate(
'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value',
'WITH value
 MERGE (t:Tweet{id:value.id})
 ON CREATE SET t += {
   postedTimestamp: datetime({ epochMillis: apoc.date.parse(value.postedTime, "ms", "yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'")}),
   text: value.text,
   language: value.twitter_lang,
   retweetCount: value.retweetCount,
   favoritesCount: value.favoritesCount,
   verb: value.verb
 }',
{batchSize:500})
YIELD *;




//Creating the Re-Tweet Nodes
CALL apoc.periodic.iterate(
'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value',
'WITH value 
 MATCH (rt:Tweet{id:value.id, verb:"share"})
 MERGE (t:Tweet{id:value.object.id})
 ON CREATE SET t += {
   postedTimestamp: datetime({ epochMillis: apoc.date.parse(value.postedTime, "ms", "yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'")}),
   text: value.object.text,
   language: value.object.twitter_lang,
   retweetCount: value.object.retweetCount,
   favoritesCount: value.object.favoritesCount,
   verb: value.object.verb
 }
 MERGE (rt)-[:RETWEETS]->(t)',
{batchSize:500})
YIELD *;



//Creating User Nodes
CALL apoc.periodic.iterate(
'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value',
'WITH value
 MATCH (t:Tweet{id:value.id})
 MERGE (u:User{id:value.actor.id})
 ON CREATE SET u += {
   displayName: value.actor.displayName,
   preferredUsername: value.actor.preferredUsername 
 }
 MERGE (u)-[:POSTS]->(t)
 ',
{batchSize:500})
YIELD *;



//Creating User Nodes
CALL apoc.periodic.iterate(
'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value',
'WITH value
 MATCH (t:Tweet{id:value.object.id, verb:"post"})
 MERGE (u:User{id:value.object.actor.id})
 ON CREATE SET u += {
   displayName: value.object.actor.displayName,
   preferredUsername: value.object.actor.preferredUsername
 }
 MERGE (u)-[:POSTS]->(t)
 ',
{batchSize:500})
YIELD *;



// Creating the relationship MENTIONS between the Tweet and User Nodes
CALL apoc.periodic.iterate(
  'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value RETURN value',
  'WITH value.id as id, value.twitter_entities.user_mentions as user_mentions_array
   MATCH (t:Tweet {id: id})
   UNWIND user_mentions_array as user_mentions
   MERGE (u:User {id: user_mentions.id_str, preferredUsername: user_mentions.screen_name, displayName: user_mentions.name})
   MERGE (t)-[:MENTIONS]->(u)',
  {batchSize: 500}
) YIELD *;




//Creating User for Retweets with Mentions Relationship
// Creating the relationship MENTIONS between the Tweet and User Nodes
CALL apoc.periodic.iterate(
  'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value RETURN value',
  'WITH value.object.id as id, value.object.twitter_entities.user_mentions as user_mentions_array
   MATCH (t:Tweet {id: id})
   UNWIND user_mentions_array as user_mentions
   MERGE (u:User {id: user_mentions.id_str, preferredUsername: user_mentions.screen_name, displayName: user_mentions.name})
   MERGE (t)-[:MENTIONS]->(u)',
  {batchSize: 500}
) YIELD *;


//Creating HashTag and Tags with the Tweet Node
CALL apoc.periodic.iterate(
  'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value RETURN value',
  'WITH value.id as id, value.twitter_entities.hashtags as hashtag_array
   MATCH (t:Tweet {id: id})
   UNWIND hashtag_array as hashtag
   MERGE (h:Hashtag {text: hashtag.text})
   MERGE (t)-[:TAGS]->(h)',
  {batchSize: 500}
) YIELD *;




//Creating Retweet and Hastag with TAGS
CALL apoc.periodic.iterate(
  'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value RETURN value',
  'WITH value.object.id as id, value.object.twitter_entities.hashtags as hashtag_array
   MATCH (t:Tweet {id: id})
   UNWIND hashtag_array as hashtag
   MERGE (h:Hashtag {text: hashtag.text})
   MERGE (t)-[:TAGS]->(h)',
  {batchSize: 500}
) YIELD *;


//Creating Source and Tweeter USING relationship
CALL apoc.periodic.iterate(
  'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value RETURN value',
  'WITH value.id as id, value.generator.displayName as displayName, value.generator.link as link 
   MATCH (t:Tweet {id: id})
   MERGE (s:Source {displayName: displayName, link:link } )
   MERGE (t)-[:USING]->(s)',
  {batchSize: 500}
) YIELD *;


//Creating Source and ReTweeter USING relationship
CALL apoc.periodic.iterate(
  'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value RETURN value',
  'WITH value.object.id as id, value.object.generator.displayName as displayName, value.object.generator.link as link 
   MATCH (t:Tweet {id: id})
   MERGE (s:Source {displayName: displayName, link:link } )
   MERGE (t)-[:USING]->(s)',
  {batchSize: 500}
) YIELD *;


//Creating Tweet and Link LINKS relationship
CALL apoc.periodic.iterate(
  'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value RETURN value',
  'WITH value.id as id, value.twitter_entities.urls as urls
   MATCH (t:Tweet {id: id})
   UNWIND urls AS url_obj
   MERGE (l:Link {expandedURL: url_obj.expanded_url, displayURL: url_obj.display_url, url: url_obj.url } )
   MERGE (t)-[:LINKS]->(l)',
  {batchSize: 500}
) YIELD *;




//Creating ReTweet and Link LINKS relationship
CALL apoc.periodic.iterate(
  'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value RETURN value',
  'WITH value.object.id as id, value.object.twitter_entities.urls as urls
   MATCH (t:Tweet {id: id})
   UNWIND urls AS url_obj
   MERGE (l:Link {expandedURL: url_obj.expanded_url, displayURL: url_obj.display_url, url: url_obj.url } )
   MERGE (t)-[:LINKS]->(l)',
  {batchSize: 500}
) YIELD *;



Part 1
MATCH (t:Tweet)-[:USING]->(s:Source), (t)<-[:POSTS]-(u:User)
RETURN s.displayName as source, COUNT(t) as num_posts, COUNT(DISTINCT u.id) as num_users
ORDER BY num_posts DESC
LIMIT 5;


Part 2



Part 3
MATCH p=shortestPath((u:User{preferredUsername:"luckyinsivan"})-[r:LINKS|MENTIONS|POSTS|RETWEETS|TAGS*]-(h:Hashtag{text:"imsosick"})) RETURN length(p) as path_length;
