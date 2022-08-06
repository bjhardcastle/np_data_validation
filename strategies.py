"""Strategies for looking up DataValidationFiles in a database and performing some action depending on context."""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, List, Union

# if TYPE_CHECKING:
import data_validation as dv


def new_file_with_checksum(subject: dv.DataValidationFile) -> dv.DataValidationFile:
    """
    Get a new file to avoid modifying the original.
    """
    checksum = subject.generate_checksum(subject.path, subject.size)
    return dv.DataValidationFile(path=subject.path, size=subject.size, checksum=checksum)


def generate_checksum(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> dv.DataValidationFile:
    """
    Generate a checksum for a file and add to database.
    """
    new_file = new_file_with_checksum(subject)
    db.add_file(new_file)
    return new_file


def generate_checksum_if_not_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB):
    """
    If the database has no entry for the subject file, generate a checksum for it.
    """
    accepted_matches = [subject.Match.SELF, subject.Match.SELF_NO_CHECKSUM]
    matches = db.get_matches(subject, match=accepted_matches)
    if not matches:
        generate_checksum(subject, db)


def ensure_checksum(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> dv.DataValidationFile:
    """
    If the database has no entry for the subject file, generate a checksum for it.
    """
    if not subject.checksum:
        subject = exchange_if_checksum_in_db(subject, db)
    if not subject.checksum:
        subject = generate_checksum(subject, db)
    return subject


def find_invalid_copies_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> List[dv.DataValidationFile]:
    """
    Check for invalid copies of the subject file in database.
    """
    matches = db.get_matches(subject)
    match_type = [(subject == match) for match in matches]
    return [
        m for i,m in enumerate(matches) 
        if match_type[i] >= subject.Match.CHECKSUM_COLLISION 
        and match_type[i] <= subject.Match.UNSYNCED_OR_CORRUPT_DATA
        ] or None


def find_valid_copies_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> List[dv.DataValidationFile]:
    """
    Check for valid copies of the subject file in database.
    """
    accepted_matches = [subject.Match.VALID_COPY_RENAMED, subject.Match.VALID_COPY_SAME_NAME]
    matches = db.get_matches(subject, match=accepted_matches)
    return matches or None
    

def exchange_if_checksum_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> dv.DataValidationFile:
    """
    If the database has an entry for the subject file that already has a checksum, swap 
    the subject for the entry in the database. Saves us regenerating checksums for large files.
    If not, return the subject.
    """
    if subject.checksum:
        return subject
    
    accepted_matches = subject.Match.SELF_NO_CHECKSUM
    matches = db.get_matches(subject, match=accepted_matches)
    
    if not matches:
        return subject
    
    if len(matches) == 1:
        return matches[0]
    else:
        # multiple matches with different checksums: should regenerate checksum
        return subject


def delete_if_valid_backup_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> int:
    """
    If the database has an entry for the subject file in known backup locations, or a new specified location, we can
    delete the subject. 
    This is just a safety measure to prevent calling 'find_valid_backups' and deleting the returned list of backups!
    """               
    subject = ensure_checksum(subject, db)
    backups = find_valid_backups(subject, db)
    if backups:
        dv.report(subject, backups)
        # a final check before deleting (all items in 'backups' should be valid copies):
        if (subject.checksum != backups[0].checksum or subject.size != backups[0].size):
            raise AssertionError(f"Not a valid backup, something has gone wrong: {subject} {backups[0]}")
        
        try:
            pathlib.Path(subject.path).unlink()
            dv.logging.info(f"DELETED {subject.path}")
            return subject.size
        except PermissionError:
            dv.logging.info(f"Permission denied: could not delete {subject.path}")
    return 0


def find_valid_backups(subject: dv.DataValidationFile, db: dv.DataValidationDB, backup_paths: Union[List[str], List[pathlib.Path]] = None) -> List[dv.DataValidationFile]:
    if not backup_paths:
        backup_paths = set()
    else:
        backup_paths = set(backup_paths)
    backup_paths.add(subject.session.lims_path.as_posix())
    backup_paths.add(subject.session.npexp_path.as_posix())
        
    if not backup_paths:
        return None
    
    subject = ensure_checksum(subject, db)
    
    invalid_backups = find_invalid_copies_in_db(subject, db)
    if invalid_backups:
        dv.report(subject, invalid_backups)
        return None
    
    matches = find_valid_copies_in_db(subject, db)
    
    backups = set()
    if matches:
        
        for match in matches:
            for backup_path in backup_paths:
                if match.path.startswith(backup_path):
                    backups.add(match)
        
    else:
        
        for backup_path in backup_paths:
            backup = db.DVFile(pathlib.Path(backup_path, subject.relative_path()).as_posix())
            if backup.accessible:
                backups.add(generate_checksum(backup, db))
    
    return list(backups) or None
    

def regenerate_checksums_on_mismatch(subject: dv.DataValidationFile, other: dv.DataValidationFile) -> None:
    """
    If the database has an entry for the subject file that has a different checksum, regenerate the checksum for it.
    """
    accepted_matches = [subject.Match.SELF, subject.Match.SELF_NO_CHECKSUM]
    #TODO regenerate and check again
    #* need a solution for replacing entries where the checksum is different

