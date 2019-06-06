import pandas as pd
from sklearn.cluster import KMeans

from beo_datastore.libs.intervalframe import ValidationFrame288


class KMeansLoadClustering(object):
    """
    Container class for algorithms that cluster load profiles.
    """

    def __init__(self, objects, frame288_type, number_of_clusters, normalize):
        """
        :param objects: list of ValidationFrame288s
        :param frame288_type: choice - "average_frame288", "minimum_frame288",
            "maximum_frame288", "total_frame288", "count_frame288"
        :param number_of_clusters: number of clusters to create
        :param normalize: True to normalize all ValidationFrame288s to create
            values ranging between -1 and 1
        """
        self.source_dataframe = self.create_source_dataframe(
            objects, frame288_type, normalize
        )
        self.cluster = self.create_cluster(
            self.source_dataframe, number_of_clusters
        )

        self.objects = objects
        self.frame288_type = frame288_type
        self.number_of_clusters = number_of_clusters
        self.normalize = normalize

    @property
    def cluster_labels(self):
        """
        Return array of indices mapping elements to cluster.
        """
        return self.cluster.labels_

    @property
    def cluster_ids(self):
        """
        Return set of unique identifiers representing each cluster.
        """
        return set(self.cluster.labels_)

    @staticmethod
    def create_source_dataframe(objects, frame288_type, normalize):
        """
        Create source dataframe to be used in clustering algorithm. Source
        dataframe consists of rows of data where each row contains 288
        datapoint representing each month-hour of a ValidationFrame288.

        :param objects: QuerySet of objects containing an intervalframe attr
            (ValidationIntervalFrame)
        :param frame288_type: choice - "average_frame288", "minimum_frame288",
            "maximum_frame288", "total_frame288", "count_frame288"
        :param normalize: True to normalize values to range between -1 and 1
        :return: pandas DataFrame
        """
        if normalize:
            return pd.DataFrame(
                [
                    getattr(
                        x.intervalframe, frame288_type
                    ).normalized_frame288.flattened_array
                    for x in objects
                ]
            )
        else:
            return pd.DataFrame(
                [
                    getattr(x.intervalframe, frame288_type).flattened_array
                    for x in objects
                ]
            )

    @staticmethod
    def create_cluster(source_dataframe, number_of_clusters):
        """
        Create KMeans cluster and run algorithm.

        :param source_dataframe: pandas DataFrame where each row is a flattened
            288
        :param number_of_clusters: int
        :return: KMeans cluster
        """
        cluster = KMeans(n_clusters=number_of_clusters)
        cluster.fit(source_dataframe)

        return cluster

    def get_reference_frame288_by_cluster_id(self, cluster_id):
        """
        Return ValidationFrame288 representing reference frame (a.k.a. best
        fit frame) for a single cluster.

        :param cluster_id: int
        :return: ValidationFrame288
        """
        return ValidationFrame288.convert_flattened_array_to_frame288(
            self.cluster.cluster_centers_[cluster_id]
        )

    def get_objects_by_cluster_id(self, cluster_id):
        """
        Return objects in a single cluster.

        :param cluster_id: identifier of cluster (int)
        :return: list of objects
        """
        return [
            x[0]
            for x in zip(self.objects, self.cluster_labels)
            if x[1] == cluster_id
        ]

    def get_cluster_frame288s_by_cluster_id(self, cluster_id):
        """
        Return a cluster's objects' ValidationFrame288 for a single cluster.

        :param cluster_id:
        :return: list of ValidationFrame288s
        """
        if self.normalize:
            return [
                getattr(
                    x.intervalframe, self.frame288_type
                ).normalized_frame288
                for x in self.get_objects_by_cluster_id(cluster_id)
            ]
        else:
            return [
                getattr(x.intervalframe, self.frame288_type)
                for x in self.get_objects_by_cluster_id(cluster_id)
            ]
