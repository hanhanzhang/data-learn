package com.sdu.data.hbase.procedure;

import java.util.Optional;

/**
 *  reference:
 *      <a href="https://cloud.tencent.com/developer/article/1666612" />
 *      <a href="https://www.cnblogs.com/yichunguo/p/12040456.html" />
 * */
public class AvlTree {

    public static <T extends Comparable<T>> TreeNode<T> insert(TreeNode<T> root, T value) {
        if (root == null) {
            return new TreeNode<>(value);
        }
        int cmp = root.value.compareTo(value);
        if (cmp == 0) {
            System.out.println("node already insert, value: " + value);
            return root;
        }
        // cmp < 0: root.value < value
        // cmp > 0: root.value > value
        if (cmp > 0) {
            root.left = insert(root.left, value);
        } else {
            root.right = insert(root.right, value);
        }
        // 插入节点可能造成二叉平衡搜索树打破平衡, 需要做旋转, 节点插入有四种情况:
        // LL(插到根节点左孩子的左子树): 向右旋转, 单旋转
        // RR(插到根节点右孩子的右子树): 向左旋转, 单旋转
        // LR(插到根节点左孩子的右子树): 先向左旋转, 再向右旋转, 双旋转
        // RL(插到根节点右孩子的左子树): 先向右旋转, 再向左旋转, 双旋转
        return balance(root);
    }

    // 删除节点, 若是存在待删除节点则删除并返回该节点, 否则返回空
    public static <T extends Comparable<T>> Optional<TreeNode<T>> remove(TreeNode<T> root, T value) {
        return Optional.empty();
    }

    public static <T extends Comparable<T>> Optional<T> search(TreeNode<T> root, T value) {
        if (root == null) {
            return Optional.empty();
        }
        if (root.value.equals(value)) {
            return Optional.of(value);
        }
        int cmp = root.value.compareTo(value);
        return cmp < 0 ? search(root.right, value) : search(root.left, value);
    }

    private static <T extends Comparable<T>> TreeNode<T> balance(TreeNode<T> root) {
        fixHeight(root);
        //  2: 说明新节点插入到左子树
        // -2: 说明新节点插入到右子树
        int balanceFactor = balanceFactor(root);
        if (balanceFactor == 2) {
            // 若新节点被插到根节点左孩子的右子树, 则需要双旋转(LR)
            if (balanceFactor(root.left) < 0) {
                // root.left子树插入新节点后高度必须+1才有可能使得balanceFactor=2
                // root.left子树平衡因子在新节点插入前, 有三种情况:
                //  1: 若插root.left右子树节点, root节点高度不变, 不符合, 故只能插入到root.left左子树, 为LL
                //  0: 新节点可插到root.left左右子节点, 故balanceFactor(root.left) < 0表明插到root.left右子树
                // -1: 若插root.left左子树节点, root节点高度不变, 不符合, 故只能插入到root.left右子树(balanceFactor(root.left) = -2), 为LR
                root.left = leftRotate(root.left);
            }
            return rightRotate(root);
        } else if (balanceFactor == -2) {
            // 若新节点被插到根节点右孩子的左子树, 则需要双旋转(RL)
            if (balanceFactor(root.right) > 0) {
                root.right = rightRotate(root.right);
            }
            return leftRotate(root);
        }
        // 未打破平衡不做任何旋转
        return root;
    }

    private static <T extends Comparable<T>> TreeNode<T> leftRotate(TreeNode<T> root) {
        // 左旋转: 根节点的右孩子节点变为新根节点, 旧根节点是新根节点的左子树, 新根节点的左孩子是旧根节点的右孩子(保序)
        TreeNode<T> newRoot = root.right;
        root.right = newRoot.left;
        newRoot.left = root;
        // 旋转后调整节点高度(先调整孩子节点高度, 再调整父节点高度)
        fixHeight(root);
        fixHeight(newRoot);
        return newRoot;
    }

    private static <T extends Comparable<T>> TreeNode<T> rightRotate(TreeNode<T> root) {
        // 右旋转: 根节点的左孩子节点变为新根节点, 旧根节点是新根节点的右子树, 新根节点的右孩子是旧根节点的左孩子(保序)
        TreeNode<T> newRoot = root.left;
        root.left = newRoot.right;
        newRoot.right = root;
        // 旋转后调整节点高度(先调整孩子节点高度, 再调整父节点高度)
        fixHeight(root);
        fixHeight(newRoot);
        return newRoot;
    }

    private static <T extends Comparable<T>> void fixHeight(TreeNode<T> root) {
        int left = height(root.left);
        int right = height(root.right);
        root.height = 1 + Math.max(left, right);
    }

    private static <T extends Comparable<T>> int height(TreeNode<T> node) {
        return node == null ? 0 : node.height;
    }

    private static <T extends Comparable<T>> int balanceFactor(TreeNode<T> node) {
        return height(node.left) - height(node.right);
    }

    public static class TreeNode<T extends Comparable<T>> {
        private int height;
        private final T value;
        private TreeNode<T> left;
        private TreeNode<T> right;

        public TreeNode(T value) {
            this.height = 0;
            this.value = value;
        }

        public int getHeight() {
            return height;
        }


        public T getValue() {
            return value;
        }

        public TreeNode<T> getLeft() {
            return left;
        }

        public TreeNode<T> getRight() {
            return right;
        }

        @Override
        public String toString() {
            return String.format("h = %d, value: %s", getHeight(), getValue());
        }
    }

}
