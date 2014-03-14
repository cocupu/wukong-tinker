Wukong.processor(:extractor) do
  def process hsh
    begin
      expanded_url = hsh["twitter_entities"]["urls"][0]["expanded_url"]
    rescue
      # do nothing
    end
    if expanded_url
      yield expanded_url
    end
  end
end