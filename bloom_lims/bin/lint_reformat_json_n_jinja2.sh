fd \.json . | parallel 'jq . {} > {}.tmp; mv {}.tmp {};'

djlint --reformat templates

