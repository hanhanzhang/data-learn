package com.sdu.data.hbase.test;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sdu.data.hbase.procedure.AvlTree;

public class AvlTreeTest {

    private AvlTree.TreeNode<Double> root;

    @Before
    public void setup() {
        /*
         *         5
         *      /     \
         *     2       6
         *   /   \
         *  1     3
         * */
        root = new AvlTree.TreeNode<>(5.0);
        AvlTree.insert(root, 6.0);
        AvlTree.insert(root, 2.0);
        AvlTree.insert(root, 3.0);
        AvlTree.insert(root, 1.0);
    }

    @Test
    public void testAvlTreeLL01() {
        /*
         *  newRoot:
         *         2
         *      /     \
         *     1       5
         *   /     /      \
         *  0.5   3        6
         * */
        AvlTree.TreeNode<Double> newRoot = AvlTree.insert(root, 0.5);
        Assert.assertEquals(2.0, newRoot.getValue(), 0);
        Assert.assertEquals(2, newRoot.getHeight(), 0);
    }

    @Test
    public void testAvlTreeLL02() {
        /*
         *  newRoot:
         *          2
         *      /       \
         *     1         5
         *       \     /   \
         *       1.5  3     6
         * */
        AvlTree.TreeNode<Double> newRoot = AvlTree.insert(root, 1.5);
        Assert.assertEquals(2.0, newRoot.getValue(), 0);
        Assert.assertEquals(2, newRoot.getHeight(), 0);
    }


    @Test
    public void testAvlTreeRR01() {
        /*
         *          5
         *      /       \
         *     2         7
         *   /   \     /   \
         *  1     3   6      8
         *                    \
         *                    10
         * */
        AvlTree.insert(root, 7.0);
        AvlTree.insert(root, 8.0);
        AvlTree.insert(root, 10.0);
        /*
         * 插入11节点旋转:
         *          7
         *        /   \
         *       5     8
         *     /  \      \
         *    2    6      10
         *  /   \           \
         * 1     3           11
         * */
        AvlTree.TreeNode<Double> newRoot = AvlTree.insert(root, 11.0);
        Assert.assertEquals(7.0, newRoot.getValue(), 0);
        Assert.assertEquals(3, newRoot.getHeight(), 0);
    }

    @Test
    public void testAvlTreeLR01() {
        /*
         *  newRoot:
         *         3
         *      /     \
         *     2       5
         *   /   \      \
         *  1    2.5     6
         * */
        AvlTree.TreeNode<Double> newRoot = AvlTree.insert(root, 2.5);
        Assert.assertEquals(3.0, newRoot.getValue(), 0);
        Assert.assertEquals(2, newRoot.getHeight(), 0);
    }

    @Test
    public void testAvlTreeLR02() {
        /*
         *  newRoot:
         *         3
         *      /     \
         *     2       5
         *   /        /  \
         *  1        4    6
         * */
        AvlTree.TreeNode<Double> newRoot = AvlTree.insert(root, 4.0);
        Assert.assertEquals(3.0, newRoot.getValue(), 0);
        Assert.assertEquals(2, newRoot.getHeight(), 0);
    }

    @Test
    public void testAvlTreeRL01() {
        /*
         *          5
         *      /       \
         *     2         6
         *   /   \     /
         *  1     3   5.5
         *          /     \
         *        5.3    5.6
         * */
        AvlTree.insert(root, 5.5);
        AvlTree.insert(root, 5.3);
        AvlTree.insert(root, 5.6);
        /*
         * 插入5.2节点旋转, 注意: 虽然是RL, 但是只旋转一次就满足平衡条件
         * newRoot:
         *              5
         *          /       \
         *         2         5.5
         *     /     \     /     \
         *    1      3   5.3      6
         *              /       /
         *             5.2     5.6
         * */
        AvlTree.TreeNode<Double> newRoot = AvlTree.insert(root, 5.2);
        Assert.assertEquals(5.5, newRoot.getValue(), 0);
    }

    @Test
    public void testAvlTreeSearch() {
        Optional<Double> leafValue = AvlTree.search(root, 1.0);
        Assert.assertTrue(leafValue.isPresent());
        Assert.assertEquals(1.0, leafValue.get(), 0);

        Optional<Double> middleValue = AvlTree.search(root, 2.0);
        Assert.assertTrue(middleValue.isPresent());
        Assert.assertEquals(2.0, middleValue.get(), 0);
    }
}
