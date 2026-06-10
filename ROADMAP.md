# Future Roadmap

## Wishlist for 1.0 (anytime in year 2025)

- [x] Ensure that runs on 1000+ node clusters result in a functional tool without major hurdles
- [x] Test against several environments and deployment modes automatically (rhel?/ubuntu/debian X pip/git/virtualenv X python2.{5,6,7})
- [x] Add support for SLURM - need at least 3 clusters traces to get things started - and human help
- [ ] Add support for LSF - need at least 3 clusters traces to get things started - and human help
- [ ] Dissociate presentation from calculation (currently it is tic-tac in a lockstep)
- [x] Improve troubleshooting by better reporting and do best-effort approach when input is clunky
- [x] Export state in JSON format online
- [ ] Provide EasyBuild easyconfig so that delivery is possible in that way, too

## Selected Implementation Items for Challenge #7

### ✅ 1. SLURM Support Enhancement
- Enhanced SLURM backend validation with comprehensive test samples
- Added support for multiple SLURM cluster configurations (basic, large_cluster, large_mixed, etc.)
- Integrated SLURM validation into CI/CD pipeline with sample gates

### ✅ 2. JSON Export and Better Error Reporting  
- Maintained existing JSON export functionality (`-E` flag)
- Enhanced error reporting through fortifications.py and validation tools
- Added comprehensive logging and debugging capabilities
- Improved troubleshooting with detailed validation artifacts
