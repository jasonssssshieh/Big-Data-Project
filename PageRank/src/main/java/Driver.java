public class Driver{
	public void main(String[] args) throws Exception {
		UnitMultiplication multiplication = new UnitMultiplication();
		UnitSum unitSum = new UnitSum();

		/*
		args0 dir of transition.txt
		agrs1 dir of PageRank.txt
		args2 dir of unitMultiplication output
		args3 times of convergence
		args4 beta
		*/
		String transitionMatrix = args[0];
		String prMatrix = args[1];
		String unitState = args[2];
		int count = Integer.parseInt(args[3]);
		String beta = args[4];
		for(int i = 0; i < count; ++i){
			String[] args1 = {transitionMatrix, prMatrix + i, unitState + i, beta};
			multiplication.main(args1);
			String[] args2 = {unitState + i, prMatrix+(i+1), beta};
			unitSum.main(args2);
		}
	}
}