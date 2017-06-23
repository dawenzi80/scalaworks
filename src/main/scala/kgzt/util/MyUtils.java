package kgzt.util;

import java.io.File;

/**
 * Created by liuxw on 2017/6/21.
 */
public class MyUtils {

    /**
     * 删除指定的文件或者文件夹
     * @param pathname
     * @return
     */
    public static int DelFileOrFolder(String pathname)
    {
        if(pathname==null)
            return 0;
        File f = new File(pathname);
        int delfilecount = 0;
        if (f.exists() && f.isFile())// 是文件则直接删除
        {
            MyLogger.LOGGER.info("Delete File:" + f.getAbsolutePath());
            f.delete();
            delfilecount++;
        }else if (f.exists() && f.isDirectory())// 是文件夹
        {
            File[] files = f.listFiles();
            if (files == null)
                return 0;
            int temp = 0;
            //先递归删除下面的所有文件和文件夹
            for (int i = 0; i < files.length; i++)
            {
                if (files[i].isDirectory())
                {
                    delfilecount = delfilecount + DelFileOrFolder(files[i].getAbsolutePath());
                }else
                {
                    MyLogger.LOGGER.info("Delete File:" + files[i].getAbsolutePath());
                    temp = files[i].delete() ? (delfilecount++) : 0;
                }
            }
            //然后把自己删除
            f.delete();
        }
        return delfilecount;
    }
}
