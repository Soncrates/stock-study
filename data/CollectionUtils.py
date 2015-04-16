class CollectionUtils(object) :
  def flatten_collection(collection_of_collections) :
      import itertools
      ret = itertools.chain(*collection_of_collections)
      return list(ret)
