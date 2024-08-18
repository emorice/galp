import galp; import tests.steps; galp.run([tests.steps.identity(i) for i in range(250)], store="/tmp/store")
