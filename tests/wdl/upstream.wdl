task upstream {
  command {
    echo 'Hello' > out.txt
    echo 'Hello' >> out.txt
    echo 'Hello' >> out.txt
  }

  output {
    File out_file = "out.txt"
  }
}

workflow wf {
  call upstream
}
