# Returns a report on how many times each URL has been tweeted.
# Yields JSON containing key, count, retweets and total tweets.  Example:
#   {"key":"http://nyti.ms/1eaCmuG","count":1,"retweets":95,"total_tweets":96}
Wukong.processor(:mapper) do
  
  def process hsh
    url_entities = []
    begin
      url_entities = hsh["gnip"]["urls"]
    end
    url_entities.each do |url_entity|
      extracted = {"url" => url_entity["expanded_url"], "retweets"=>hsh["retweetCount"]}
      yield extracted.to_json
    end
  end
end

Wukong.processor(:reducer, Wukong::Processor::Accumulator) do

  attr_accessor :count, :retweets
  
  # Group records based on matching url values
  def get_key(record)
    record["url"]
  end
  
  def start record
    self.count = 0
    self.retweets = 0
  end
  
  def accumulate record
    self.count += 1
    self.retweets += record["retweets"].to_i
  end

  def finalize
    to_return = {url:key, count:count, retweets:retweets, total_tweets:count+retweets}
    yield JSON.generate(to_return)
  end
end
