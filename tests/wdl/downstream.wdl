task downstream {
	input {
		File in_file
	}

	command {
		wc -l < ${in_file}
		}

	output {
		Int n_lines = read_int(stdout())
		}
}

workflow wf {
	call downstream
}
