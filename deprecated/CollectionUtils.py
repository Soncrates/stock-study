class CollectionUtils(object) :
  @staticmethod
  def flatten_collection(collection_of_collections) :
      import itertools
      ret = itertools.chain(*collection_of_collections)
      return list(ret)
  @staticmethod
  def chunkify(data, size=100) :
      return [data[x:x+size] for x in range(0, len(data), size)]
