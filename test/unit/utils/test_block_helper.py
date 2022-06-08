from unittest import TestCase
from utils.block_helper import Block, BoundingBox, Geometry, Point, Relationship, extend_polygon, get_points


class BlockHelperTest(TestCase):

    def test_bounding_box(self):
        bounding_box = BoundingBox(10, 10, 5, 5)
        self.assertTrue(hasattr(bounding_box, "Width"))
        self.assertTrue(hasattr(bounding_box, "Top"))
        self.assertTrue(hasattr(bounding_box, "Left"))
        self.assertTrue(hasattr(bounding_box, "Height"))

        bounding_box_extend = BoundingBox(10, 10, 10, 15)
        bounding_box.extend_bounding_box(bounding_box_extend)
        self.assertEqual(bounding_box.__dict__, BoundingBox(15, 20, 5, 5).__dict__)

    def test_point(self):
        point = Point(5, 10)
        self.assertTrue(hasattr(point, "X"))
        self.assertTrue(isinstance(point.X, int))
        self.assertTrue(hasattr(point, "Y"))
        self.assertTrue(isinstance(point.Y, int))

    def test_get_points(self):
        points = get_points(10, 20, 5, 10)
        self.assertEqual(len(points), 4)
        self.assertEqual(points[0].__dict__, Point(5, 10).__dict__)
        self.assertEqual(points[1].__dict__, Point(15, 10).__dict__)
        self.assertEqual(points[2].__dict__, Point(15, 30).__dict__)
        self.assertEqual(points[3].__dict__, Point(5, 30).__dict__)
    
    def test_extend_polygon(self):
        polygon = get_points(10, 20, 5, 10)
        polygon_extend = get_points(5, 15, 15, 30)
        self.assertListEqual([p.__dict__ for p in extend_polygon(polygon, polygon_extend)], [p.__dict__ for p in get_points(15, 35, 5, 10)])

    def test_geometry(self):
        geometry = Geometry(10, 20, 5, 10)
        self.assertTrue(hasattr(geometry, "BoundingBox"))
        self.assertTrue(isinstance(geometry.BoundingBox, BoundingBox))
        self.assertEqual(geometry.BoundingBox.__dict__, BoundingBox(10, 20, 5, 10).__dict__)
        self.assertTrue(hasattr(geometry, "Polygon"))
        self.assertTrue(isinstance(geometry.Polygon, list))
        self.assertEqual(len(geometry.Polygon), 4)
        self.assertListEqual([p.__dict__ for p in geometry.Polygon], [p.__dict__ for p in get_points(10, 20, 5, 10)])

        geometry_extend = Geometry(10, 10, 10, 25)
        geometry.extend_geometry(geometry_extend)
        self.assertEqual(geometry.BoundingBox.__dict__, BoundingBox(15, 25, 5, 10).__dict__)
        self.assertListEqual([p.__dict__ for p in geometry.Polygon], [p.__dict__ for p in get_points(15, 25, 5, 10)])

    def test_relationship(self):
        relationship = Relationship(["id1", "id2", "id3"], "CHILD")
        self.assertTrue(hasattr(relationship, "Ids"))
        self.assertTrue(hasattr(relationship, "Type"))
        self.assertTrue(isinstance(relationship.Ids, list))
        self.assertEqual(len(relationship.Ids), 3)
        self.assertEqual(relationship.Type, "CHILD")

        relationship = Relationship([], "CHILD")
        self.assertEqual(len(relationship.Ids), 0)

    def test_block(self):
        block_word = Block(1, "WORD", "Some_text.", 0, Geometry(10, 20, 5, 10), 0)
        self.assertTrue(hasattr(block_word, "BlockType"))
        self.assertTrue(isinstance(block_word.BlockType, str))
        self.assertTrue(hasattr(block_word, "Id"))
        self.assertTrue(isinstance(block_word.Id, str))
        self.assertTrue(hasattr(block_word, "Text"))
        self.assertTrue(isinstance(block_word.Text, str))
        self.assertTrue(hasattr(block_word, "Geometry"))
        self.assertTrue(isinstance(block_word.Geometry, Geometry))
        self.assertTrue(hasattr(block_word, "Relationships"))
        self.assertTrue(isinstance(block_word.Relationships, list))
        self.assertTrue(hasattr(block_word, "Page"))
        self.assertTrue(isinstance(block_word.Page, int))
        self.assertTrue(hasattr(block_word, "parentBlockIndex"))
        self.assertTrue(block_word.parentBlockIndex == 0)
        self.assertTrue(hasattr(block_word, "blockIndex"))
        self.assertTrue(isinstance(block_word.blockIndex, int))

        block_line = Block(1, "LINE", "Some text.", 0)
        self.assertTrue(block_line.Geometry is None)
        self.assertTrue(block_line.parentBlockIndex == -1)

        block_word.extend_geometry(Geometry(10, 10, 10, 25))
        self.assertEqual(block_word.Geometry.BoundingBox.__dict__, BoundingBox(15, 25, 5, 10).__dict__)
        self.assertListEqual([p.__dict__ for p in block_word.Geometry.Polygon], [p.__dict__ for p in get_points(15, 25, 5, 10)])
