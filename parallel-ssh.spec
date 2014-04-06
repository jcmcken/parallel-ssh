# sitelib for noarch packages, sitearch for others (remove the unneeded one)
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}
%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)")}

Name:           parallel-ssh
Version:        3.3.0
Release:        1%{?dist}
Summary:        Parallel SSH utilities

Group:          Utilities
License:        BSD
URL:            https://github.com/jcmcken/parallel-ssh
Source0:        %{name}-%{version}.tar.gz

BuildArch:      noarch
Requires:       python(abi) >= 2.4
Requires:       python2
BuildRequires:  python-devel

%description
Parallel SSH utilities, including parallel SSH, SCP, reverise SCP, and more


%prep
%setup -q


%build
# Remove CFLAGS=... for noarch packages (unneeded)
CFLAGS="$RPM_OPT_FLAGS" %{__python} setup.py build


%install
rm -rf $RPM_BUILD_ROOT
%{__python} setup.py install -O1 --skip-build --root $RPM_BUILD_ROOT

# remove setup.py distributed man page
rm -f $RPM_BUILD_ROOT%{_prefix}/man/man1/pssh*

%{__install} -d -m 0755 $RPM_BUILD_ROOT%{_mandir}/man1
%{__install} -m 0755 man/man1/pssh.1 $RPM_BUILD_ROOT%{_mandir}/man1

 
%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
%attr(0755,root,root) %{_bindir}/*
%doc COPYING
%doc ChangeLog
%doc README.rst 
%doc AUTHORS
%doc %{_mandir}/man1/*
# For noarch packages: sitelib
%{python_sitelib}/*

%changelog
* Sun Apr 06 2014 Jon McKenzie - 3.3.0
- Initial RPM release
