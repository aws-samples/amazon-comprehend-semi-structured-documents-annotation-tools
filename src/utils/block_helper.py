# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Block helper class."""
from typing import List, Union
import uuid
import copy


class DictionaryClass:
    """Class to define an object as a dictionary."""

    def __str__(self):
        """Return a string representation of the object as a dictionary."""
        return str(self.__dict__)

    def __repr__(self):
        """Return a string representation of the object."""
        return str(self)

    def reprJSON(self):
        """Return dictionary representation of the object."""
        return self.__dict__


def JSONHandler(Obj):
    """Return a JSON representation from an object."""
    if hasattr(Obj, 'reprJSON'):
        return Obj.reprJSON()
    else:
        raise TypeError('Object of type %s with value of %s is not JSON serializable' % (type(Obj), repr(Obj)))


class BoundingBox(DictionaryClass):
    """Class to define a Semi-Structured BoundingBox object."""

    def __init__(self, width: int, height: int, left: int, top: int):
        self.Width = width
        self.Top = top
        self.Left = left
        self.Height = height

    def extend_bounding_box(self, bounding_box: 'BoundingBox'):
        """Extend the BoundingBox object's dimensions to another BoundingBox object."""
        selfRight = self.Left + self.Width
        selfBottom = self.Top + self.Height
        bbRight = bounding_box.Left + bounding_box.Width
        bbBottom = bounding_box.Top + bounding_box.Height
        self.Width = abs(min(self.Left, bounding_box.Left) - max(selfRight, bbRight))
        self.Height = abs(min(self.Top, bounding_box.Top) - max(selfBottom, bbBottom))
        if self.Left > bounding_box.Left:
            self.Left = bounding_box.Left
        if self.Top > bounding_box.Top:
            self.Top = bounding_box.Top


class Point(DictionaryClass):
    """Class to define a Semi-Structured Point object."""

    def __init__(self, x: int, y: int):
        self.X = x
        self.Y = y


def get_points(width: int, height: int, left: int, top: int):
    """Return a list of Point objects from a boundary."""
    return [Point(left, top), Point(left + width, top), Point(left + width, top + height), Point(left, top + height)]


def extend_polygon(polygon1: List[Point], polygon2: List[Point]):
    """Return a polygon extended from another polygon."""
    bounding_box_1 = BoundingBox(polygon1[2].X - polygon1[0].X, polygon1[2].Y - polygon1[0].Y, polygon1[0].X, polygon1[0].Y)
    bounding_box_2 = BoundingBox(polygon2[2].X - polygon2[0].X, polygon2[2].Y - polygon2[0].Y, polygon2[0].X, polygon2[0].Y)
    bounding_box_1.extend_bounding_box(bounding_box_2)

    return get_points(bounding_box_1.Width, bounding_box_1.Height, bounding_box_1.Left, bounding_box_1.Top)


class Geometry(DictionaryClass):
    """Class to define an Semi-Structured Block object."""

    def __init__(self, width: int, height: int, left: int, top: int):
        self.BoundingBox = BoundingBox(width, height, left, top)
        self.Polygon = get_points(width, height, left, top)

    def extend_geometry(self, geometry: 'Geometry'):
        """Extend the Geometry object's BoundingBox to another Geometry object."""
        self.BoundingBox.extend_bounding_box(geometry.BoundingBox)
        self.Polygon = extend_polygon(self.Polygon, geometry.Polygon)


class Relationship(DictionaryClass):
    """Class to define an Semi-Structured Relationship object."""

    def __init__(self, ids: List[str], type: str):
        self.Ids = ids if ids else []
        self.Type = type


class Block(DictionaryClass):
    """Class to define an Semi-Structured Block object."""

    def __init__(self, page: int, block_type: str, text: str, index: int, geometry: Union[Geometry, None] = None, parent_block_index=-1):
        self.BlockType = block_type
        self.Id = str(uuid.uuid4())
        self.Text = text
        self.Geometry = geometry
        self.Relationships: List[Relationship] = []
        self.Page = page

        self.parentBlockIndex = parent_block_index
        self.blockIndex = index

    def extend_geometry(self, geometry: Geometry):
        """Extend the Block object's Geometry to another Geometry object."""
        if not self.Geometry:
            self.Geometry = copy.deepcopy(geometry)
        else:
            self.Geometry.extend_geometry(geometry)
