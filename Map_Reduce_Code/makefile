everything: compile_map_reduce run_map_reduce move_output_files

compile_map_reduce:
	mpicxx -std=c++11 -fopenmp map_reduce.cpp -o map_reduce

run_map_reduce:
	mpiexec -n 2 ./map_reduce

submit_qsub:
	qsub -n map_reduce_1_process.sub
	qsub -n map_reduce_2_processes.sub
	qsub -n map_reduce_2_processes_2.sub
	qsub -n map_reduce_4_processes.sub
	qsub -n map_reduce_4_processes_2.sub
	qsub -n map_reduce_8_processes.sub
	qsub -n map_reduce_8_processes_2.sub

move_output_files:
	mkdir OutputDir
	mv Process_* OutputDir/

clean:
	rm -rf OutputDir
	rm map_reduce
	rm *~
