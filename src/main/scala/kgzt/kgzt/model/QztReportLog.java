package kgzt.kgzt.model;

import java.io.Serializable;

/**
 * Created by liuxw on 2017/6/23.
 */


public class QztReportLog  implements Serializable , Comparable{
    private String model = "";
    private int num=0;

    public QztReportLog(String model) {
        this.model = model;
        this.num=1;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return model+"__"+num;
    }

    @Override
    public int compareTo(Object o) {
        return this.num>((QztReportLog)o).getNum()?0:1;
    }
}
