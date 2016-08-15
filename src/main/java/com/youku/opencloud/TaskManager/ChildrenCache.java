package com.youku.opencloud.TaskManager;

import java.util.List;
import java.util.ArrayList;

/**
 * Auxiliary cache to handle changes to the lists of tasks and of workers.
 */

public class ChildrenCache {
    protected List<String> children;
    
    ChildrenCache() {
        this.children = null;        
    }
    
    ChildrenCache(List<String> children) {
        this.children = children;        
    }
        
    List<String> getList() {
        return children;
    }
        
    /**
     * update and return children added
     * @param newChildren
     * @return children who added
     */
    List<String> addedAndSet( List<String> newChildren) {
        ArrayList<String> diff = null;
        
        if(children == null) {
            diff = new ArrayList<String>(newChildren);
        } else {
            for(String s: newChildren) {
                if(!children.contains( s )) {
                    if(diff == null) {
                        diff = new ArrayList<String>();
                    }
                
                    diff.add(s);
                }
            }
        }
        this.children = newChildren;
            
        return diff;
    }
     
    /**
     * update and return children removed
     * @param newChildren
     * @return children who removed
     */
    List<String> removedAndSet( List<String> newChildren) {
        List<String> diff = null;
            
        if(children != null) {
            for(String s: children) {
                if(!newChildren.contains( s )) {
                    if(diff == null) {
                        diff = new ArrayList<String>();
                    }
                    
                    diff.add(s);
                }
            }
        }
        this.children = newChildren;
        
        return diff;
    }
}
