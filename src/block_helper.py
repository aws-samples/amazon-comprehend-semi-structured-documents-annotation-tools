# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Block helper class."""
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
    """Class to define a SSIE BoundingBox object."""

    def __init__(self, width, height, left, top):
        self.Width = width
        self.Top = top
        self.Left = left
        self.Height = height

    def extend_bounding_box(self, boundingBox):
        """Extend the BoundingBox object's dimensions to another BoundingBox object."""
        selfRight = self.Left + self.Width
        selfBottom = self.Top + self.Height
        bbRight = boundingBox.Left + boundingBox.Width
        bbBottom = boundingBox.Top + boundingBox.Height
        self.Width = abs(min(self.Left, boundingBox.Left) - max(selfRight, bbRight))
        self.Height = abs(min(self.Top, boundingBox.Top) - max(selfBottom, bbBottom))
        if self.Left > boundingBox.Left:
            self.Left = boundingBox.Left
        if self.Top > boundingBox.Top:
            self.Top = boundingBox.Top


class Point(DictionaryClass):
    """Class to define a SSIE Point object."""

    def __init__(self, x, y):
        self.X = x
        self.Y = y


def get_points(width, height, left, top):
    """Return a list of Point objects from a boundary."""
    return [Point(left, top), Point(left + width, top), Point(left + width, top + height), Point(left, top + height)]


class Geometry(DictionaryClass):
    """Class to define an SSIE Block object."""

    def __init__(self, width, height, left, top):
        self.BoundingBox = BoundingBox(width, height, left, top)
        self.Polygon = get_points(width, height, left, top)

    def extend_geometry(self, geometry):
        """Extend the Geometry object's BoundingBox to another Geometry object."""
        self.BoundingBox.extend_bounding_box(geometry.BoundingBox)


class Relationship(DictionaryClass):
    """Class to define an SSIE Relationship object."""

    def __init__(self, ids, type):
        self.Ids = ids if ids else []
        self.Type = type


class Block(DictionaryClass):
    """Class to define an SSIE Block object."""

    def __init__(self, page, block_type, text, index, geometry=None, parentBlockIndex=-1):
        self.BlockType = block_type
        self.Id = str(uuid.uuid4())
        self.Text = text
        self.Geometry = geometry
        self.Relationships = []
        self.Page = page

        self.parentBlockIndex = parentBlockIndex
        self.blockIndex = index

    def extend_geometry(self, geometry):
        """Extend the Block object's Geometry to another Geometry object."""
        if not self.Geometry:
            self.Geometry = copy.deepcopy(geometry)
        else:
            self.Geometry.extend_geometry(geometry)
