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
   MERGE (t)-[:CONTAINS]->(l)',
  {batchSize: 500}
) YIELD *;




//Creating ReTweet and Link LINKS relationship
CALL apoc.periodic.iterate(
  'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value RETURN value',
  'WITH value.object.id as id, value.object.twitter_entities.urls as urls
   MATCH (t:Tweet {id: id})
   UNWIND urls AS url_obj
   MERGE (l:Link {expandedURL: url_obj.expanded_url, displayURL: url_obj.display_url, url: url_obj.url } )
   MERGE (t)-[:CONTAINS]->(l)',
  {batchSize: 500}
) YIELD *;



MATCH(n:Tweet{id:'149715659408689070080'}) RETURN n


Part 1
MATCH (t:Tweet)-[:USING]->(s:Source), (t)<-[:POSTS]-(u:User)
RETURN s.displayName as source, COUNT(t) as num_posts, COUNT(DISTINCT u.id) as num_users
ORDER BY num_posts DESC
LIMIT 5;


// Part 2
MATCH (u:User)-[:POSTS]->(t:Tweet)-[:TAGS]->(h:Hashtag)
WHERE t.retweetCount > 50
WITH u, COUNT(t) AS num_pop_posts, h
ORDER BY h.text
WITH u, num_pop_posts, COLLECT(h.text) AS all_hashtags
WITH u, num_pop_posts, all_hashtags
ORDER BY num_pop_posts DESC
LIMIT 3
WITH u.preferredUsername AS user_name, 
     num_pop_posts, 
     apoc.coll.frequencies(all_hashtags) AS freqs
UNWIND freqs AS freq
RETURN user_name, num_pop_posts, freq.item ORDER BY freq.count DESC LIMIT 3;




Part 3
// Output the number the path size of 24
MATCH p=shortestPath((u:User{preferredUsername:"luckyinsivan"})-[r:CONTAINS|MENTIONS|POSTS|RETWEETS|TAGS*]-(h:Hashtag{text:"imsosick"})) RETURN length(p) as path_length;

// You could visualize the graph as well.
MATCH p=shortestPath((u:User{preferredUsername:"luckyinsivan"})-[r:CONTAINS|MENTIONS|POSTS|RETWEETS|TAGS*]-(h:Hashtag{text:"imsosick"}))
RETURN p, length(p) as path_length;



// Question 3
// Part 1
// Of course! Let's say you want to find out who's sharing the most links from a specific website like "realestate.com.au" on Twitter. Your current database setup doesn't make this easy to do.

// What I'm Suggesting
// I'm suggesting that we add a new category, or "node" as it's called in database terms, just for website domains like "realestate.com.au" or "twitter.com". This new node will be connected to each link that is shared in a tweet.

// Why Do This?
// By adding this new category for domains, you can very quickly and easily find out things like:

// Who shared the most links from "realestate.com.au"?
// What are the most popular websites people are linking to on Twitter?
// How This Works
// Right now, you have a "Tweet" that "contains" a "Link", right?

// What I'm suggesting is adding one more step. So it would go like this:

// A "Tweet" "contains" a "Link"
// That "Link" "belongs to" a "Domain"
// Example:
// Let's say we have a tweet that contains the link "https://realestate.com.au/house123".

// The tweet "contains" the link "https://realestate.com.au/house123".
// The link "https://realestate.com.au/house123" "belongs to" the domain "realestate.com.au".
// By doing this, if you want to find all the tweets that contain links from "realestate.com.au", you can easily do that because all those links are connected to the domain "realestate.com.au" in your database.

// In Simple Steps:
// When you add a new tweet into your database, you also check what link it contains.
// You find out the domain of that link (like "realestate.com.au").
// You add that domain to your database if it isn't already there.
// You make a note in your database that the link in the tweet belongs to that domain.
// This is pretty much a way to make your database smarter and more organized, so it's easier to get the information you want later on.




// Part 2
// Creating Domain and Link BELONGS_TO relationship
CALL apoc.periodic.iterate(
    'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value RETURN value',
    'WITH value.id as id, value.twitter_entities.urls as urls
     MATCH (t:Tweet {id: id})
     UNWIND urls AS url_obj
     // Extract the domain from the expanded URL
     WITH t, url_obj, split(url_obj.expanded_url, "/")[2] as domain
     // Create or Merge the Domain node
     MERGE (d:Domain {name: domain})
     // Create or Merge the Link node
     MERGE (l:Link {expandedURL: url_obj.expanded_url, displayURL: url_obj.display_url, url: url_obj.url } )
     // Create or Merge the relationships
     MERGE (t)-[:CONTAINS]->(l)
     MERGE (l)-[:BELONGS_TO]->(d)',
    {batchSize: 500}
  ) YIELD *;
  
  
  
  // Creating Domain and Link BELONGS_TO relationship for Retweets
  CALL apoc.periodic.iterate(
    'CALL apoc.load.json("https://raw.githubusercontent.com/sai-sarva/Tweet-Analysis/main/tweets_clean.json") YIELD value RETURN value',
    'WITH value.object.id as id, value.object.twitter_entities.urls as urls
     MATCH (t:Tweet {id: id})
     UNWIND urls AS url_obj
     // Extract the domain from the expanded URL
     WITH t, url_obj, split(url_obj.expanded_url, "/")[2] as domain
     // Create or Merge the Domain node
     MERGE (d:Domain {name: domain})
     // Create or Merge the Link node
     MERGE (l:Link {expandedURL: url_obj.expanded_url, displayURL: url_obj.display_url, url: url_obj.url } )
     // Create or Merge the relationships
     MERGE (t)-[:CONTAINS]->(l)
     MERGE (l)-[:BELONGS_TO]->(d)',
    {batchSize: 500}
  ) YIELD *;
  

//   Part 3
