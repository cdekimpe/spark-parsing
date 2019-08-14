/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package me.dekimpe.types;
import java.io.Serializable;

/**
 *
 * @author Coreuh
 */
public class PageLink implements Serializable {
    
    private int pl_id;
    private String pl_title;

    public int getId() {
        return pl_id;
    }

    public String getTitle() {
        return pl_title;
    }

    public void setId(int pl_id) {
        this.pl_id = pl_id;
    }

    public void setTitle(String pl_title) {
        this.pl_title = pl_title;
    }
    
}
