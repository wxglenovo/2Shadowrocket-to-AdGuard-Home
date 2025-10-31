import os
import dns.resolver

def validate_rule(rule, validated_file, log_file):
    resolver = dns.resolver.Resolver()
    resolver.timeout = 5
    resolver.lifetime = 5

    domain = rule.split('^')[0].replace("*", "")
    try:
        resolver.resolve(domain)
        with open(validated_file, "a", encoding="utf-8") as f_valid:
            f_valid.write(rule + "\n")
    except:
        with open(log_file, "a", encoding="utf-8") as f_log:
            f_log.write(f"Failed to resolve: {domain} - {rule}\n")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python validate.py <part_X.txt> <validated_part_X.txt> <log_file>")
        sys.exit(1)

    part_file = sys.argv[1]
    validated_file = sys.argv[2]
    log_file = sys.argv[3]

    if os.path.exists(part_file):
        with open(part_file, "r", encoding="utf-8") as f:
            rules = f.readlines()
            for rule in rules:
                validate_rule(rule.strip(), validated_file, log_file)
    else:
        print(f"File {part_file} does not exist")
