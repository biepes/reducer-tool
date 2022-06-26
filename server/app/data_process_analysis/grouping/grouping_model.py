from enum import Enum


class Grouping_enum(Enum):
    PEARSON = 1

class Group_linkage_enum(Enum):
    COMPLETE = 1

class Grouping_model():
    group_linkage: int
    cut_height: float

    def __init__(self, group_linkage, cut_height):
        self.group_linkage = group_linkage
        self.cut_height = cut_height
