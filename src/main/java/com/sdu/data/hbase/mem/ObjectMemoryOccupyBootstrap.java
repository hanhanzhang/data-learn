package com.sdu.data.hbase.mem;

import org.openjdk.jol.info.ClassLayout;

public class ObjectMemoryOccupyBootstrap {

    // 内存占用:
    // 1. 对象头
    //    8(若是32位操作系统则为4) + 8(若是开启指针压缩则为4)
    // 2. 实例数据
    //
    // 3. 填充数据
    public static class Student {

        public final String name;
        public final int age;
        public final Teacher chineseTeacher;
        public final Teacher mathTeacher;

        public Student(String name, int age, Teacher chineseTeacher, Teacher mathTeacher) {
            this.name = name;
            this.age = age;
            this.chineseTeacher = chineseTeacher;
            this.mathTeacher = mathTeacher;
        }
    }

    public static class Teacher {
        public final String name;
        public final long phone;

        public Teacher(String name, long phone) {
            this.name = name;
            this.phone = phone;
        }
    }

    public static void main(String[] args) {
        Student student = new Student("jame", 28, new Teacher("wade", 18349232349L), new Teacher("lily", 13458739876L));
        ClassLayout classLayout = ClassLayout.parseInstance(student);
        System.out.println("instance size: " + classLayout.instanceSize());
        classLayout.fields().forEach(f -> System.out.printf("field name: %s, size: %d\n", f.name(), f.size()));
        System.out.println(classLayout.toPrintable());
    }

}
