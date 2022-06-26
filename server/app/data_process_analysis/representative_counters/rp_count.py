import pandas as pd


class Representative_counters:

    counters: list
    groups: list
    distance_groups: list

    def __init__(self, groups: list = None, distance_groups: list = None):
        self.counters = list()
        self.groups = groups
        self.distance_groups = distance_groups

    def execute(self) -> list:
        new_groups = self.__get_sliced_groups(self.groups[0], self.groups[1])
        for i in range(2, len(self.groups)):
                new_groups = self.__get_sliced_groups(new_groups, self.groups[i])

        return self.get_representative_counters(new_groups, self.distance_groups)

    def __get_sliced_groups(self, g1, g2):
        groups = list()
        
        for i in range(len(g1)):
            for j in range(len(g2)):
                if (len(g1[i]) > 0) and (len(g2[j]) > 0):
                    intersection = g1[i] & g2[j]
                    if (len(intersection) > 0):
                        g1[i] -= intersection
                        g2[j] -= intersection
                        groups.append(intersection)

        for i in range(len(g1)):
            if len(g1[i]) > 0:
                groups.append(g1[i])

        for i in range(len(g2)):
            if len(g2[i]) > 0:
                groups.append(g2[i])

        return groups

    def get_representative_counters(self, groups: list, distance_groups: list):
        representative_counters = list()

        for group in groups:
            last_sum = float('inf')
            last_counter = None

            for counter in group:
                dissimilarity_sum = 0
                for other_counter in group:
                    if not counter == other_counter:
                        for dissimilarity_matrix in distance_groups:
                            if counter in dissimilarity_matrix and other_counter in dissimilarity_matrix:
                                dissimilarity_sum += dissimilarity_matrix[counter][other_counter]
                            else:
                                dissimilarity_sum += 1

                if dissimilarity_sum < last_sum:
                    last_sum = dissimilarity_sum
                    last_counter = counter
            
            representative_counters.append(last_counter)
        
        return representative_counters
