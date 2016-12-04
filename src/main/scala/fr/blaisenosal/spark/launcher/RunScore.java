package fr.blaisenosal.spark.launcher;

import fr.blaisenosal.spark.EmailArchiever;

public class RunScore {

	public static void main(String[] args) {
		boolean flag = false;
		if(args.length == 2) {
			try {
				Integer min = Integer.parseInt(args[0]);
				Integer max = Integer.parseInt(args[1]);
				
				flag = true;
				EmailArchiever.runScore(min, max);
			} catch(NumberFormatException e) {
				System.exit(1);
			}
		} else if(args.length == 1) {
			// Score pour un seul utilisateur
			EmailArchiever.runScore(Integer.parseInt(args[0]));
			flag = true;
		}
		
		if(!flag) {
		System.out.println("Erreur d'usage... ");
		System.exit(1);
		}
	}

}
