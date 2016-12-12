package fr.blaisenosal.spark.launcher;

import fr.blaisenosal.spark.EmailArchiever;

/**
 * Created by antoine-xubuntu on 04/12/16.
 */
public class LaunchEmailArchiever {

    public static void main (String[] args) {
        if(args.length == 1)
            System.setProperty("user.dir", args[0]);

        EmailArchiever.main(args);
    }

}
