.PHONY: validate package phase1 tables phase2
validate:
	./infra/scripts/validate.sh
package:
	./infra/scripts/package.sh
phase1:
	./infra/scripts/deploy.sh phase1
tables:
	./infra/scripts/bootstrap-tables.sh
phase2:
	@test -n "$(MSK_BOOTSTRAP_SERVERS)" || (echo "Set MSK_BOOTSTRAP_SERVERS" && exit 1)
	./infra/scripts/deploy.sh phase2 "$(MSK_BOOTSTRAP_SERVERS)"
