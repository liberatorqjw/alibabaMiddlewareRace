package com.alibaba.middleware.race.sort;

import java.util.Comparator;

/**
 * Created by qinjiawei on 16-7-3.
 */
public class SetComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
        Long a = (Long) o1;
        Long b = (Long) o2;

        if ( a > b)
            return 1;
        else if( a == b) {
            return 0;

        }
        else {
            return -1;
        }
    }
}
