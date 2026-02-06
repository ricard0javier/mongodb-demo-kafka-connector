sp.listConnections();

catalogue_collection = {
  connectionName: "source_collection",
  db: "asos",
  coll: "products",
};

sp.process([
  {
    $source: catalogue_collection,
  },
  {
    $tumblingWindow: {
      interval: {
        size: 5,
        unit: "second",
      },
      pipeline: [
        {
          $group: {
            _id: "documentKey",
            maxPrice: { $max: "$updateDescription.updatedFields.price" },
          },
        },
      ],
    },
  },
]);
